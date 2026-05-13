//! POC: io_uring recv with a kernel-owned slot
//!
//! Demonstrates the core idea: a fixed buffer registered with the kernel,
//! a state machine that prevents Rust from reading the buffer while the
//! kernel owns it, and a completion path that hands ownership back to
//! userspace without copying.
//!
//! Run with:
//!   cargo run --example io_uring_recv
//!
//! This does NOT use the ring buffer machinery yet — it's a single slot
//! to prove the kernel-writes-into-your-buffer story end to end.

use std::cell::UnsafeCell;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU8, Ordering};

use io_uring::{opcode, types, IoUring};

// --- Slot state machine ---------------------------------------------------

const FREE: u8 = 0;
const KERNEL_OWNED: u8 = 1;
const COMPLETE: u8 = 2;

const BUF_SIZE: usize = 4096;

struct IoSlot {
    state: AtomicU8,
    buf: UnsafeCell<[u8; BUF_SIZE]>,
    len: AtomicU8, // bytes written by kernel, saturates at 255 for this POC
}

// SAFETY: we enforce exclusive access through the state machine.
unsafe impl Sync for IoSlot {}

impl IoSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(FREE),
            buf: UnsafeCell::new([0u8; BUF_SIZE]),
            len: AtomicU8::new(0),
        }
    }

    /// Returns a raw pointer to the buffer for SQE construction.
    /// Caller must transition state to KERNEL_OWNED before submitting.
    fn as_ptr(&self) -> *mut u8 {
        self.buf.get().cast::<u8>()
    }

    fn mark_kernel_owned(&self) {
        self.state.store(KERNEL_OWNED, Ordering::Release);
    }

    /// Called by the completion reaper. Transitions KERNEL_OWNED -> COMPLETE
    /// and records how many bytes the kernel wrote.
    ///
    /// # Safety
    /// Must only be called after the kernel has signalled completion (CQE arrived).
    unsafe fn mark_complete(&self, bytes: usize) {
        self.len.store(bytes.min(255) as u8, Ordering::Relaxed);
        self.state.store(COMPLETE, Ordering::Release);
    }

    /// Returns the filled bytes. Only valid in COMPLETE state.
    fn bytes(&self) -> &[u8] {
        assert_eq!(self.state.load(Ordering::Acquire), COMPLETE);
        let len = self.len.load(Ordering::Relaxed) as usize;
        // SAFETY: state == COMPLETE means the kernel is done and no SQE is live.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), len) }
    }

    fn mark_free(&self) {
        self.state.store(FREE, Ordering::Release);
    }
}

// --- Experiment -----------------------------------------------------------

fn main() -> anyhow::Result<()> {
    // Bind a listener on an ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    println!("listening on {addr}");

    // Spawn a thread that connects and sends a message.
    std::thread::spawn(move || {
        // Give the main thread a moment to submit the recv before we send.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut stream = TcpStream::connect(addr).expect("connect");
        stream
            .write_all(b"hello from the sender thread")
            .expect("write");
        println!("[sender] sent");
    });

    // Accept the connection in blocking mode — we'll recv via io_uring.
    let (conn, peer) = listener.accept()?;
    println!("accepted connection from {peer}");

    let fd = {
        use std::os::unix::io::AsRawFd;
        conn.as_raw_fd()
    };

    // Set up a minimal io_uring with a single-entry SQ/CQ.
    let mut ring = IoUring::new(4)?;

    // Our single slot.
    let slot = IoSlot::new();

    // Register the buffer with the kernel so it can DMA directly into it.
    // SAFETY: slot lives on the stack for the duration of this function,
    // and we unregister (implicitly, ring drop) before returning.
    unsafe {
        ring.submitter().register_buffers(&[libc::iovec {
            iov_base: slot.as_ptr().cast(),
            iov_len: BUF_SIZE,
        }])?;
    }

    // Build the SQE: recv into registered buffer index 0.
    slot.mark_kernel_owned();

    let recv_e = opcode::RecvFixed::new(
        types::Fd(fd),
        slot.as_ptr(),
        BUF_SIZE as u32,
        0, // registered buffer index
    )
    .build()
    .user_data(42); // our request id

    // SAFETY: SQE is valid; slot is KERNEL_OWNED so no Rust reference exists.
    unsafe {
        ring.submission().push(&recv_e)?;
    }

    // Submit and wait for 1 completion.
    ring.submit_and_wait(1)?;

    // Reap the CQE.
    let cqe = ring.completion().next().expect("expected a CQE");
    let request_id = cqe.user_data();
    let result = cqe.result();

    assert_eq!(request_id, 42, "unexpected user_data");
    assert!(result >= 0, "recv failed: {}", result);

    // Transition slot to COMPLETE — kernel is done.
    // SAFETY: CQE arrived, kernel will not touch this buffer again.
    unsafe { slot.mark_complete(result as usize) };

    // Now we can read the bytes — no copy, straight from the slot.
    let received = slot.bytes();
    println!(
        "received {} bytes: {:?}",
        received.len(),
        std::str::from_utf8(received).unwrap_or("<non-utf8>")
    );

    slot.mark_free();

    // Unregister buffers before slot is dropped.
    ring.submitter().unregister_buffers()?;

    Ok(())
}

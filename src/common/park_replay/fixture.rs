//! The model graph exported by `lake exe park_graph`.

use std::collections::{HashMap, HashSet};
use std::process::Command;

/// The configuration a fixture was generated for, from its `c` line.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub variant: String,
    /// Whether each producer leases a park slot.
    pub leases: Vec<bool>,
    pub releases: usize,
    pub closes: bool,
}

/// One model edge: source, target, and the actor label.
pub type Edge = (usize, usize, String);

pub struct ModelGraph {
    pub name: String,
    pub config: RunConfig,
    /// State encoding → (id, whether its successors were exported).
    states: HashMap<String, (usize, bool)>,
    edges: HashSet<Edge>,
}

impl ModelGraph {
    /// Exports configuration `name` from the current model by running
    /// `lake exe park_graph <name>` in `proofs/`.
    ///
    /// # Panics
    ///
    /// Panics if `lake` cannot run, the export fails, or its output is
    /// malformed.
    pub fn load(name: &str) -> Self {
        let proofs = concat!(env!("CARGO_MANIFEST_DIR"), "/proofs");
        let output = Command::new("lake")
            .args(["exe", "park_graph", name])
            .current_dir(proofs)
            .output()
            .unwrap_or_else(|e| panic!("running `lake exe park_graph {name}`: {e}"));
        assert!(
            output.status.success(),
            "`lake exe park_graph {name}` failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout)
            .unwrap_or_else(|e| panic!("`lake exe park_graph {name}`: {e}"));
        Self::parse(name, &text)
    }

    fn parse(name: &str, text: &str) -> Self {
        let mut config = None;
        let mut states = HashMap::new();
        let mut edges = HashSet::new();
        for line in text.lines() {
            let mut fields = line.splitn(3, ' ');
            let kind = fields.next().unwrap_or_default();
            let first = fields.next().unwrap_or_default();
            let rest = fields.next().unwrap_or_default();
            match kind {
                "c" => config = Some(Self::config(line)),
                "s" | "x" => {
                    states.insert(rest.to_owned(), (Self::number(first), kind == "s"));
                }
                "e" => {
                    let mut tail = rest.split(' ');
                    let to = Self::number(tail.next().unwrap_or_default());
                    let actor = tail.next().unwrap_or_default().to_owned();
                    edges.insert((Self::number(first), to, actor));
                }
                _ => panic!("{name}: unexpected line {line:?}"),
            }
        }
        Self {
            name: name.to_owned(),
            config: config.unwrap_or_else(|| panic!("{name}: no `c` line")),
            states,
            edges,
        }
    }

    fn number(field: &str) -> usize {
        field
            .parse()
            .unwrap_or_else(|_| panic!("not a number: {field:?}"))
    }

    fn config(line: &str) -> RunConfig {
        let fields: Vec<&str> = line.split(' ').collect();
        assert_eq!(fields.len(), 6, "malformed `c` line {line:?}");
        RunConfig {
            variant: fields[1].to_owned(),
            leases: fields[2].chars().map(|c| c == 'L').collect(),
            releases: Self::number(fields[3]),
            closes: fields[4] == "1",
        }
    }

    /// The id of the state with this encoding, and whether the export
    /// expanded it.
    pub fn state(&self, encoding: &str) -> Option<(usize, bool)> {
        self.states.get(encoding).copied()
    }

    pub fn has_edge(&self, edge: &Edge) -> bool {
        self.edges.contains(edge)
    }

    /// The edges not in `used`, sorted.
    pub fn unused(&self, used: &HashSet<Edge>) -> Vec<Edge> {
        let mut unused: Vec<Edge> = self.edges.difference(used).cloned().collect();
        unused.sort();
        unused
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn state_count(&self) -> usize {
        self.states.len()
    }

    /// The encoding of state `id`, for reports.
    pub fn encoding(&self, id: usize) -> &str {
        self.states
            .iter()
            .find(|(_, (state, _))| *state == id)
            .map_or("?", |(encoding, _)| encoding)
    }
}

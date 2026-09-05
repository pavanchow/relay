//! Hand-rolled FNV-1a (64-bit). No external crate, fully deterministic.

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// One-shot FNV-1a over a byte slice.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = Hasher::new();
    h.write(bytes);
    h.finish()
}

/// Incremental accumulator so a cache key can fold together many pieces
/// (config fields plus file contents) into one deterministic digest.
#[derive(Clone)]
pub struct Hasher {
    state: u64,
}

impl Hasher {
    pub fn new() -> Self {
        Hasher { state: FNV_OFFSET }
    }

    pub fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state ^= b as u64;
            self.state = self.state.wrapping_mul(FNV_PRIME);
        }
    }

    /// Length-delimited so "ab" + "c" never collides with "a" + "bc".
    pub fn write_str(&mut self, s: &str) {
        self.write_u64(s.len() as u64);
        self.write(s.as_bytes());
    }

    pub fn write_u64(&mut self, v: u64) {
        self.write(&v.to_le_bytes());
    }

    pub fn finish(&self) -> u64 {
        self.state
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Hasher::new()
    }
}

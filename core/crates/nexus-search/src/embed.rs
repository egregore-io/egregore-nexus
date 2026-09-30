//! The embedding seam. Semantic search turns text into a fixed-dimension vector and
//! asks optional plugin vector storage for nearest messages
//! ([`nexus_store::search_index::vector_top_k`]). Real embeddings (a model server,
//! a local ONNX runtime, …) are pluggable behind the [`Embedder`] trait; this crate
//! ships only a **deterministic hash-based stub** so the pipeline is exercisable
//! without any ML dependency. The stub is intentionally crude — same text always
//! yields the same vector, and token overlap nudges vectors closer — enough to
//! test fallback/ranking seams, not to do real recall.

/// The fixed dimensionality of stub embeddings. Real embedders may differ; the store stores whatever
/// width it is handed (libSQL `F32_BLOB`), so the only requirement is consistency within a corpus.
pub const STUB_DIM: usize = 64;

/// Turns text into a fixed-dimension embedding vector. Implementations must be deterministic for a
/// given input (the store compares by cosine distance, so stable vectors are required for stable
/// results).
pub trait Embedder: Send + Sync {
    /// Embed `text` into a vector. The returned length must be stable across calls for one embedder.
    fn embed(&self, text: &str) -> Vec<f32>;
}

/// A dependency-free, deterministic embedder: a normalized bag-of-token-hashes over a fixed
/// [`STUB_DIM`]-wide space. Tokens are split on non-alphanumeric boundaries and lowercased, so
/// texts that share words land in overlapping dimensions (and thus closer in cosine distance).
#[derive(Debug, Clone, Default)]
pub struct StubEmbedder;

impl StubEmbedder {
    /// Construct the stub embedder.
    pub fn new() -> Self {
        StubEmbedder
    }
}

impl Embedder for StubEmbedder {
    fn embed(&self, text: &str) -> Vec<f32> {
        let mut v = vec![0.0f32; STUB_DIM];
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
        {
            let h = fnv1a(&token.to_ascii_lowercase());
            let idx = (h as usize) % STUB_DIM;
            // sign from a second hash bit so distinct tokens don't all pile up positively.
            let sign = if (h >> 33) & 1 == 0 { 1.0 } else { -1.0 };
            v[idx] += sign;
        }
        normalize(&mut v);
        v
    }
}

/// 64-bit FNV-1a hash — small, fast, deterministic, no deps.
fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// L2-normalize in place (no-op for the zero vector, which stays zero).
fn normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_is_deterministic_and_fixed_width() {
        let e = StubEmbedder::new();
        let a = e.embed("rebase the auth refactor");
        let b = e.embed("rebase the auth refactor");
        assert_eq!(a, b);
        assert_eq!(a.len(), STUB_DIM);
    }

    #[test]
    fn shared_tokens_are_nearer_than_disjoint() {
        let e = StubEmbedder::new();
        let q = e.embed("auth token refactor");
        let near = e.embed("the auth refactor");
        let far = e.embed("deploy the kubernetes cluster");
        let cos = |x: &[f32], y: &[f32]| -> f32 { x.iter().zip(y).map(|(a, b)| a * b).sum() };
        assert!(
            cos(&q, &near) > cos(&q, &far),
            "token overlap must pull vectors closer"
        );
    }
}

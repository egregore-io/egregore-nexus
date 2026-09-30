//! Human first-name generation for auto-named agents.
//!
//! A `nexus launch` with no `--name` gets a friendly human first name (e.g. `marcus`, `amber`)
//! instead of a `claude-ab12` token — readable, and (when picked against the live roster via
//! [`unique_first_name`]) collision-free. The whole point of auto-naming was to stop two launches
//! both grabbing `claude`; first names keep that property while being nicer to address.

/// A curated pool of common, easy-to-say/type first names (mixed origin). Large enough that random
/// collisions are rare on their own; the launch path additionally skips names already in use.
pub const FIRST_NAMES: &[&str] = &[
    "aaron", "ada", "adam", "alan", "alex", "alice", "amber", "amos", "andre", "anna", "aria",
    "arthur", "asa", "aubrey", "august", "aurora", "ava", "beatrice", "ben", "bianca", "blake",
    "bruno", "caleb", "cara", "carl", "cecil", "celia", "chloe", "clara", "cody", "cora", "cyrus",
    "dahlia", "daisy", "dana", "dante", "dara", "dean", "delia", "dexter", "diana", "dora",
    "dylan", "edith", "edwin", "elena", "eli", "ellie", "elsa", "emil", "emma", "enzo", "esme",
    "ezra", "fabian", "faye", "felix", "finn", "flora", "ford", "freya", "gabe", "gemma", "glen",
    "grace", "greta", "hana", "harvey", "hazel", "henry", "hugo", "ida", "igor", "ines", "iris",
    "isaac", "ivan", "ivy", "jack", "jade", "jasper", "jed", "jenna", "joel", "jonah", "june",
    "kai", "kara", "kate", "kira", "lana", "leo", "lila", "linus", "liv", "logan", "lola", "louis",
    "luca", "luna", "mabel", "marcus", "maria", "mason", "maya", "mila", "milo", "mira", "morgan",
    "nadia", "nash", "nell", "neil", "nina", "noah", "nora", "olive", "omar", "opal", "oscar",
    "otto", "paloma", "pedro", "percy", "piper", "quinn", "rafe", "remy", "rhea", "rita", "roman",
    "rosa", "rowan", "ruby", "rufus", "sage", "saul", "selma", "silas", "simon", "sofia", "tara",
    "theo", "tilda", "tobias", "uma", "vera", "victor", "violet", "wade", "willa", "robin", "yara",
    "yuki", "zane", "zara", "zoe",
];

/// FNV-1a hash of a seed → a stable index spread across the pool.
fn seed_index(seed: &str) -> usize {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in seed.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) % FIRST_NAMES.len()
}

/// Pick a first name deterministically from a seed (e.g. a session id) — same seed → same name,
/// different seeds spread across the pool. Used by the daemon when a caller omits a name and the
/// live roster isn't consulted.
pub fn first_name_for_seed(seed: &str) -> String {
    FIRST_NAMES[seed_index(seed)].to_string()
}

/// Pick a first name avoiding any in `taken` (case-insensitive), starting from a seed-derived
/// position and scanning the pool. Only if every pool entry is in use does it fall back to
/// `<name><n>` (>150 concurrent agents) — so the common case is always a clean first name.
pub fn unique_first_name(seed: &str, taken: &[String]) -> String {
    let taken_lc: std::collections::HashSet<String> =
        taken.iter().map(|s| s.to_lowercase()).collect();
    let start = seed_index(seed);
    for i in 0..FIRST_NAMES.len() {
        let cand = FIRST_NAMES[(start + i) % FIRST_NAMES.len()];
        if !taken_lc.contains(cand) {
            return cand.to_string();
        }
    }
    let base = FIRST_NAMES[start];
    for n in 2.. {
        let cand = format!("{base}{n}");
        if !taken_lc.contains(&cand) {
            return cand;
        }
    }
    unreachable!("integer overflow before exhausting name suffixes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_is_deterministic() {
        assert_eq!(first_name_for_seed("s_abc"), first_name_for_seed("s_abc"));
    }

    #[test]
    fn unique_skips_taken() {
        // Force a collision on the seed's first pick, expect a different pool name.
        let first = first_name_for_seed("seed1");
        let out = unique_first_name("seed1", &[first.clone()]);
        assert_ne!(out, first);
        assert!(FIRST_NAMES.contains(&out.as_str()));
    }

    #[test]
    fn unique_falls_back_to_suffix_when_pool_exhausted() {
        let taken: Vec<String> = FIRST_NAMES.iter().map(|s| s.to_string()).collect();
        let out = unique_first_name("seedX", &taken);
        // Every bare name is taken → a numbered variant of the seed's base name.
        assert!(out.chars().last().unwrap().is_ascii_digit());
    }
}

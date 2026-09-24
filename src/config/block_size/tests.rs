use super::{BlockSizePolicy, MAX_BLOCK_SIZE};

/// A size the writer refuses is refused where the policy is built, on the
/// caller's thread: carried into the tree, it would panic the writer's size
/// setter inside a background flush instead, and fail every retry of it.
#[test]
#[should_panic(expected = "block size must be <= 4 MiB")]
fn a_size_over_the_block_limit_is_refused_when_the_policy_is_built() {
    let _ = BlockSizePolicy::all(MAX_BLOCK_SIZE + 1);
}

/// The same bound holds for every level of a per-level policy.
#[test]
#[should_panic(expected = "block size must be <= 4 MiB")]
fn a_level_over_the_block_limit_is_refused_when_the_policy_is_built() {
    let _ = BlockSizePolicy::new([4_096, MAX_BLOCK_SIZE + 1]);
}

/// The limit itself is a valid size, at every level.
#[test]
fn a_size_at_the_block_limit_is_accepted() {
    assert_eq!(BlockSizePolicy::all(MAX_BLOCK_SIZE).get(3), MAX_BLOCK_SIZE);
    assert_eq!(
        BlockSizePolicy::new([4_096, MAX_BLOCK_SIZE]).get(1),
        MAX_BLOCK_SIZE
    );
}

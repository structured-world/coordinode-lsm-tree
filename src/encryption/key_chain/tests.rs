use super::*;

#[test]
fn static_chain_returns_installed_key() {
    let chain = StaticKeyChain::new().with_key(7, [0x42; 32]);
    assert_eq!(chain.key(7), Some(&[0x42; 32]));
}

#[test]
fn static_chain_missing_epoch_returns_none() {
    let chain = StaticKeyChain::new().with_key(7, [0x42; 32]);
    assert!(chain.key(8).is_none());
}

#[test]
fn with_key_replaces_existing_epoch() {
    let chain = StaticKeyChain::new()
        .with_key(1, [0x01; 32])
        .with_key(1, [0x02; 32]);
    assert_eq!(chain.key(1), Some(&[0x02; 32]));
    assert_eq!(chain.len(), 1);
}

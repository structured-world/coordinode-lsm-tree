fn main() {
    let zstd = std::env::var("CARGO_FEATURE_ZSTD").is_ok();
    let zstd_pure = std::env::var("CARGO_FEATURE_ZSTD_PURE").is_ok();

    if zstd || zstd_pure {
        println!("cargo:rustc-cfg=zstd_any");
    }
}

fn main() {
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_ZSTD");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_IO_URING");

    if std::env::var("CARGO_FEATURE_ZSTD").is_ok() {
        println!("cargo:rustc-cfg=zstd_any");
    }

    // The triple the crate is built for, which names the runner cargo starts
    // its test binary through (`CARGO_TARGET_<TRIPLE>_RUNNER`): a test that
    // runs a child of its own starts it the same way.
    if let Ok(target) = std::env::var("TARGET") {
        println!("cargo:rustc-env=LSM_TREE_TARGET={target}");
    }

    // The `io-uring` feature builds `IoUringFs` only where its dependency does:
    // Linux, on the architectures `Cargo.toml` lists for it. Elsewhere the
    // default config runs on `StdFs`.
    let on = |name: &str, values: &[&str]| {
        std::env::var(name).is_ok_and(|value| values.contains(&value.as_str()))
    };
    if std::env::var("CARGO_FEATURE_IO_URING").is_ok()
        && on("CARGO_CFG_TARGET_OS", &["linux"])
        && on(
            "CARGO_CFG_TARGET_ARCH",
            &["x86_64", "aarch64", "riscv64", "loongarch64", "powerpc64"],
        )
    {
        println!("cargo:rustc-cfg=io_uring_fs");
    }
}

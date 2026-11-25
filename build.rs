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
    // The vector kernels (xxh3's, and the longest shared prefix's), only
    // where the target's ABI carries the vector registers: a soft-float target
    // (`x86_64-unknown-none`, `i686-unknown-uefi`,
    // `aarch64-unknown-none-softfloat`) leaves them off, the compiler refuses
    // a function that enables them there, and the scalar kernels run.
    let features = std::env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    let has = |feature: &str| features.split(',').any(|f| f == feature);
    // (cfg, architecture, byte orders it runs in, the feature it needs).
    let kernels = [
        ("xxh3_sse2", "x86_64", &["little"][..], "sse2"),
        ("xxh3_neon", "aarch64", &["little"][..], "neon"),
        ("lsp_neon", "aarch64", &["little"][..], "neon"),
    ];
    for (cfg, arch, endians, feature) in kernels {
        if on("CARGO_CFG_TARGET_ARCH", &[arch])
            && on("CARGO_CFG_TARGET_ENDIAN", endians)
            && has(feature)
        {
            println!("cargo:rustc-cfg={cfg}");
        }
    }
    // The x86 longest-prefix kernels are chosen at run time, so they are
    // compiled wherever the compiler accepts them, whatever the target's
    // baseline: a 32-bit target without SSE2 in its baseline
    // (`i586-unknown-linux-gnu`) still runs them on a CPU that has it. The
    // target's features cannot tell that target from a soft-float one, which
    // lists no SSE2 either, so the compiler is asked. Unanswered (no compiler
    // to run), the target's own features decide.
    if on("CARGO_CFG_TARGET_ARCH", &["x86_64", "x86"])
        && target_accepts_feature("sse2").unwrap_or_else(|| has("sse2"))
    {
        println!("cargo:rustc-cfg=lsp_x86");
    }

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

/// Whether the compiler accepts, for the target being built, a function that
/// enables `feature`: it refuses one on a target whose ABI leaves the
/// registers of that feature out. `None` when the compiler could not be run.
///
/// Compiled as the crate is: for the same target, with the same flags.
fn target_accepts_feature(feature: &str) -> Option<bool> {
    use std::process::{Command, Stdio};

    let rustc = std::env::var_os("RUSTC")?;
    let target = std::env::var_os("TARGET")?;
    let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR")?);
    let source = out_dir.join(format!("accepts_{feature}.rs"));
    std::fs::write(
        &source,
        format!(
            "#![no_std]\n#![allow(warnings)]\n\
             #[target_feature(enable = \"{feature}\")]\npub unsafe fn probe() {{}}\n"
        ),
    )
    .ok()?;

    let mut compiler = Command::new(rustc);
    compiler
        .arg("--crate-name")
        .arg(format!("accepts_{feature}"))
        .arg("--crate-type=lib")
        .arg("--emit=metadata")
        .arg("--target")
        .arg(target)
        .arg("--out-dir")
        .arg(&out_dir);
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        compiler.args(flags.split('\x1f').filter(|flag| !flag.is_empty()));
    }
    let status = compiler
        .arg(&source)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?;
    Some(status.success())
}

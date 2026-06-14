# Contributing

## License

By contributing to this project, you agree that your contributions will be licensed under the project's license (Apache-2.0).

Thank you for your contribution!

## Changing the on-disk format

Any PR that adds or changes an on-disk format feature must satisfy the
Benchmark Symmetry Invariant: a new feature is OFF by default, has a
RocksDB-equivalent, or provides a wire-identical OFF mode, and new opt-ins are
disabled in the `RocksDbParity` bench preset so head-to-head comparisons stay
apples-to-apples. See [docs/BENCHMARKING.md](docs/BENCHMARKING.md) for the rule,
the preset matrix, and the per-PR checklist.

## Looking for issues?

https://github.com/structured-world/coordinode-lsm-tree/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22

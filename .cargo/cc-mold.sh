#!/bin/sh
# Linker driver for x86_64 Linux: links with mold where it is installed, and
# as rustc would otherwise (cc with its bundled lld) where it is not, such as
# a cross-compilation container. mold writes far less to disk than lld.
if command -v mold >/dev/null 2>&1; then
    exec cc "$@" -fuse-ld=mold
fi
exec cc "$@"

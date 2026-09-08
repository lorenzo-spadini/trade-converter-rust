#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TARGET=x86_64-pc-windows-gnu
DESTINATION="$ROOT/dist/windows/NRDToParquet.exe"

command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1 || {
    printf 'Missing MinGW cross-compiler: install mingw-w64 first.\n' >&2
    exit 1
}
rustup target list --installed | grep -qx "$TARGET" || rustup target add "$TARGET"

cd "$ROOT"
CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER=x86_64-w64-mingw32-gcc \
    cargo build --release --target "$TARGET"
mkdir -p "$(dirname "$DESTINATION")"
cp "$ROOT/target/$TARGET/release/trade-converter-rust.exe" "$DESTINATION"
printf 'Built %s\n' "$DESTINATION"

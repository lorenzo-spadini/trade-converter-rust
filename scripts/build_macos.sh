#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST="$ROOT/dist/macos"
BIN="$DIST/trade-converter-rust"
ARCHIVE="$DIST/trade-converter-rust-macOS-arm64.zip"
STAGE=$(mktemp -d "${TMPDIR:-/tmp}/nrdtoparquet-build.XXXXXX")
STAGED_BIN="$STAGE/trade-converter-rust"
trap 'rm -rf "$STAGE"' EXIT

cd "$ROOT"
cargo build --release
cp "$ROOT/target/release/trade-converter-rust" "$STAGED_BIN"
chmod +x "$STAGED_BIN"

mkdir -p "$DIST"
rm -f "$ARCHIVE"
cp "$STAGED_BIN" "$BIN"
COPYFILE_DISABLE=1 ditto -c -k --keepParent "$STAGED_BIN" "$ARCHIVE"
printf 'Built %s\nBuilt %s\n' "$BIN" "$ARCHIVE"

#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST="$ROOT/dist/macos"
APP="$DIST/NRDToParquet.app"
ARCHIVE="$DIST/NRDToParquet-macOS-arm64.zip"
STAGE=$(mktemp -d "${TMPDIR:-/tmp}/nrdtoparquet-build.XXXXXX")
STAGED_APP="$STAGE/NRDToParquet.app"
trap 'rm -rf "$STAGE"' EXIT

cd "$ROOT"
cargo build --release
mkdir -p "$STAGED_APP/Contents/MacOS"
cp "$ROOT/target/release/trade-converter-rust" "$STAGED_APP/Contents/MacOS/NRDToParquet"
cp "$ROOT/resources/Info.plist" "$STAGED_APP/Contents/Info.plist"
chmod +x "$STAGED_APP/Contents/MacOS/NRDToParquet"
xattr -cr "$STAGED_APP"
codesign --force --deep --sign - "$STAGED_APP"
codesign --verify --deep --strict "$STAGED_APP"

mkdir -p "$DIST"
rm -f "$ARCHIVE"
COPYFILE_DISABLE=1 ditto -c -k --keepParent "$STAGED_APP" "$ARCHIVE"
rm -rf "$APP"
COPYFILE_DISABLE=1 ditto "$STAGED_APP" "$APP"
printf 'Built %s\nBuilt %s\n' "$APP" "$ARCHIVE"

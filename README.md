# NRDToParquet Rust

Standalone Rust desktop converter for NRDToCSV futures data. Python is not used
by the application at build time or runtime.

## Run

Launch the GUI:

```bash
cargo run --release
```

Run the same core from the CLI for benchmarks or automation:

```bash
cargo run --release -- SOURCE DESTINATION [--tick-size 0.25] [--metrics metrics.json]
```

Source can be one CSV or a contract folder such as `NQ SEP26`. Destination must
already exist. Inputs are opened read-only. Outputs are first written as
`.partial` files and renamed only after successful completion.

## Build

macOS:

```bash
./scripts/build_macos.sh
```

The application is created at `dist/macos/NRDToParquet.app`.
The same build also creates the distribution archive
`dist/macos/NRDToParquet-macOS-arm64.zip`. The bundle is ad-hoc signed and
verified in a temporary staging directory before it is copied to `dist/`.

Windows PowerShell, executed on Windows:

```powershell
.\scripts\build_windows.ps1
```

The standalone executable is created at `dist\windows\NRDToParquet.exe`.

An x86-64 Windows GNU executable can also be cross-built from macOS when
`mingw-w64` and the Rust `x86_64-pc-windows-gnu` target are installed:

```bash
./scripts/build_windows_cross.sh
```

Cross-compilation checks source and link compatibility but does not replace a
runtime test on an actual Windows machine.

## Tests

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

All generated fixtures, benchmarks, regression outputs and reports remain under
`test_outputs/`. The separate Python project and original `test_data` are used
only as read-only golden references during regression.

## Data representation

- Prices are parsed without floating point and stored as `decimal128(38,12)`.
- Source wall time is interpreted with the IANA `Europe/Berlin` timezone.
- Output timestamps use `timestamp[ns, tz=America/Chicago]` and retain the full
  100 ns source offset.
- RAW and TRADES are streamed in 50,000-row Parquet row groups with Zstd.
- Bid/Ask remain null when their BBO side is absent; no sentinel is written.
- Temporary Bid/Ask state is reset at the start of every CSV in folder mode;
  contract-level sequence IDs, writers, and totals remain continuous.
- Tick Size is optional and affects validation only.

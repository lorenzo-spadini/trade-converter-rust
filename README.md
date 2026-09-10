# NRDToParquet Rust

Headless Rust CLI converter for NRDToCSV futures data, designed as a
deterministic worker controlled by a Python pipeline. Python is not required by
the Rust executable itself.

## Pipeline CLI

Convert exactly one strict `YYYYMMDD.csv` file:

```bash
trade-converter-rust convert-day CSV_PATH DAILY_OUTPUT_DIR \
  --tick-size 0.25 [--metrics daily_metrics.json]
```

The CSV parent folder supplies the futures contract. For example, parent folder
`NQ SEP26` maps to `NQ_09-26`. The output directory must already exist. A
strictly positive finite Tick Size is mandatory and is parsed exactly without
floating point. A successful conversion creates only:

```text
YYYYMMDD.parquet
YYYYMMDD_validation.json
```

Success is reported as the final significant stdout line:

```text
PARQUET_DONE|contract|date|parquet_path|validation_path|rows
```

Merge all verified daily files in a canonical contract directory:

```bash
trade-converter-rust merge-contract DAILY_DIR CONTRACT.parquet \
  [--metrics merge_metrics.json]
```

For example, daily directory `NQ_09-26` must be merged to
`NQ_09-26.parquet`. Daily files are selected only when named
`YYYYMMDD.parquet`, validated, sorted explicitly by date, and streamed in that
order. Success is:

```text
MERGE_DONE|contract|output_path|days_merged|rows
```

Failures use `PARQUET_FAILED|...` or `MERGE_FAILED|...` on stderr and a nonzero
exit code. Running without a subcommand prints Clap help/usage. Rust never
deletes source CSV or daily Parquet files.

## Atomic Outputs

Daily conversion first writes:

```text
YYYYMMDD.partial.parquet
YYYYMMDD_validation.partial.json
```

Merge first writes `CONTRACT.partial.parquet`. Final names appear only after
successful close and structural verification. Existing final or partial output
is never overwritten. On failure, a partial file may remain for diagnosis, but
it is never reported as complete.

## Data Contract

Daily and merged Parquet files use the existing TRADES schema:

| Column | Arrow type |
| --- | --- |
| `sequence_id` | `int64` |
| `timestamp_chicago` | `timestamp[ns, tz=America/Chicago]` |
| `instrument` | `utf8` |
| `price` | `decimal128(38,12)` |
| `size` | `int64` |
| `bid` | `decimal128(38,12)` |
| `ask` | `decimal128(38,12)` |
| `aggressor` | `utf8` |

Prices are parsed without floating point. Source wall time is interpreted in
`Europe/Berlin`; the full 100 ns source offset is retained. Missing BBO sides
remain null and aggressor is `UNKNOWN`. ReplayTradeExporter represents an
uninitialized Ask/Bid with `-1.7976931348623157E+308` (`Double.MinValue`), while
Parquet uses null for that same semantic state without contaminating real
prices. That semantic match applies only when the BBO side was genuinely absent;
all real prices require exact equality.

Tick Size is supplied once through `--tick-size`, carried by
`ConversionOptions`, parsed as an exact decimal, stored in validation JSON and
used only for tick-alignment validation. Merge does not accept or use Tick Size.

Each daily restarts `sequence_id` at 1. Merge validates every daily sequence and
timestamp order, checks order across day boundaries, preserves all non-sequence
columns, and regenerates one global sequence from 1 through the final row count.
Writing uses bounded Arrow batches and Zstd compression.

## Build

macOS:

```bash
./scripts/build_macos.sh
```

The executable is created at `dist/macos/trade-converter-rust`, together with
`dist/macos/trade-converter-rust-macOS-arm64.zip`.

Windows PowerShell, executed on Windows:

```powershell
.\scripts\build_windows.ps1
```

The standalone executable is created at
`dist\windows\trade-converter-rust.exe`.

An x86-64 Windows GNU executable can also be cross-built from macOS when
`mingw-w64` and the Rust `x86_64-pc-windows-gnu` target are installed:

```bash
./scripts/build_windows_cross.sh
```

Cross-compilation verifies source and link compatibility; runtime testing still
belongs on a Windows machine.

## Verification

```bash
cargo fmt --check
cargo check
cargo test
cargo clippy --all-targets -- -D warnings
```

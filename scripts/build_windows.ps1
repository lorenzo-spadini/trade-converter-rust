$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Push-Location $Root
try {
    cargo build --release
    $Destination = Join-Path $Root "dist\windows"
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    Copy-Item "target\release\trade-converter-rust.exe" (Join-Path $Destination "trade-converter-rust.exe") -Force
    Write-Host "Built $Destination\trade-converter-rust.exe"
}
finally {
    Pop-Location
}

param(
    [string]$Url = "jdbc:polypheny://pa:@localhost:20590/public",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\..\query_lists\nested_data\nested_data_polypheny_normalized.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "polypheny_nested_results.csv"),
    [string]$AdapterName = "ncp",
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$FetchSize = 1000,
    [int]$QueryTimeout = 0,
    [string]$Only = "",
    [string]$Sql = "",
    [string]$SqlFile = "",
    [switch]$PrintRows
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\..\.."
$runner = Join-Path $PSScriptRoot "..\run_polypheny_benchmark.ps1"
$effectiveQueries = $Queries
$temporaryQueries = $null

if ($AdapterName -ne "ncp" -and -not $Sql -and -not $SqlFile) {
    $tempDir = Join-Path $root "build\nested-customer-benchmark"
    New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
    $temporaryQueries = Join-Path $tempDir "nested_data_polypheny_normalized_$AdapterName.sql"
    $content = Get-Content -Raw -Path $Queries
    $content = $content -replace "\bncp__", ($AdapterName + "__")
    Set-Content -Path $temporaryQueries -Value $content -Encoding UTF8
    $effectiveQueries = $temporaryQueries
}

$runnerParams = @{
    Url = $Url
    Queries = $effectiveQueries
    Output = $Output
    Warmups = $Warmups
    Runs = $Runs
    FetchSize = $FetchSize
    QueryTimeout = $QueryTimeout
    NoTableNameMapping = $true
}

if ($Only) {
    $runnerParams.Only = $Only
}

if ($Sql) {
    $runnerParams.Sql = $Sql
}

if ($SqlFile) {
    $runnerParams.SqlFile = $SqlFile
}

if ($PrintRows) {
    $runnerParams.PrintRows = $true
}

& $runner @runnerParams

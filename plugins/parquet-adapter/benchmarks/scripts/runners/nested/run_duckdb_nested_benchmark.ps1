param(
    [string]$DataFile = "C:\PolyData\nested_customer\nestedcustomer.parquet",
    [string]$Url = "jdbc:duckdb:",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\..\query_lists\nested_data\nested_data_duckdb.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "duckdb_nested_results.csv"),
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$FetchSize = 1000,
    [int]$QueryTimeout = 0,
    [int]$Threads = 8,
    [string]$MemoryLimit = "16GB",
    [string]$Only = "",
    [string]$Sql = "",
    [string]$SqlFile = "",
    [string]$DuckDbVersion = "1.5.2.1",
    [switch]$PrintRows
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $DataFile)) {
    throw "Nested customer Parquet file does not exist: $DataFile"
}

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\..\.."
$runner = Join-Path $PSScriptRoot "..\run_duckdb_benchmark.ps1"
$dataFileFull = [System.IO.Path]::GetFullPath((Resolve-Path $DataFile).Path)
$dataDir = Split-Path $dataFileFull -Parent

$runnerParams = @{
    Url = $Url
    DataDir = $dataDir
    NestedCustomerFile = $dataFileFull
    Queries = $Queries
    Output = $Output
    Warmups = $Warmups
    Runs = $Runs
    FetchSize = $FetchSize
    QueryTimeout = $QueryTimeout
    Threads = $Threads
    MemoryLimit = $MemoryLimit
    DuckDbVersion = $DuckDbVersion
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

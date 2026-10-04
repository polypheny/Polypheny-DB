param(
    [string]$Server = "localhost",
    [int]$Port = 20590,
    [string]$Username = "pa",
    [string]$Password = "",
    [string]$Namespace = "ncpd_document",
    [string]$Language = "mongo",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\..\query_lists\nested_data\nested_data_mql.mql"),
    [string]$Output = (Join-Path $PSScriptRoot "polypheny_mql_nested_results.csv"),
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$FetchSize = 1000,
    [int]$ApiMajor = 1,
    [int]$ApiMinor = 9,
    [string]$Only = "",
    [string]$Mql = "",
    [string]$MqlFile = "",
    [switch]$PrintRows
)

$ErrorActionPreference = "Stop"

$runner = Join-Path $PSScriptRoot "..\run_polypheny_mql_benchmark.ps1"

$runnerParams = @{
    Server = $Server
    Port = $Port
    Username = $Username
    Password = $Password
    Namespace = $Namespace
    Language = $Language
    Queries = $Queries
    Output = $Output
    Warmups = $Warmups
    Runs = $Runs
    FetchSize = $FetchSize
    ApiMajor = $ApiMajor
    ApiMinor = $ApiMinor
}

if ($Only) {
    $runnerParams.Only = $Only
}

if ($Mql) {
    $runnerParams.Mql = $Mql
}

if ($MqlFile) {
    $runnerParams.MqlFile = $MqlFile
}

if ($PrintRows) {
    $runnerParams.PrintRows = $true
}

& $runner @runnerParams

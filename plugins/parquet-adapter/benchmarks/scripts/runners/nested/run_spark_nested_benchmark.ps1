param(
    [string]$DataFile = "C:\PolyData\nested_customer\nestedcustomer.parquet",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\..\query_lists\nested_data\nested_data_spark.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "spark_nested_results.csv"),
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$Threads = 8,
    [int]$ShufflePartitions = 8,
    [string]$DriverMemory = "16g",
    [ValidateSet("executor", "driver")]
    [string]$DrainMode = "executor",
    [string]$Only = "",
    [string]$Sql = "",
    [string]$SqlFile = "",
    [string]$Image = "apache/spark:3.5.3-java17-python3",
    [string[]]$SparkConf = @(),
    [switch]$PullImage,
    [switch]$DryRun,
    [switch]$VerboseSparkLogs,
    [switch]$PrintRows
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $DataFile)) {
    throw "Nested customer Parquet file does not exist: $DataFile"
}

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\..\.."
$runner = Join-Path $PSScriptRoot "..\run_spark_benchmark.ps1"
$dataFileFull = [System.IO.Path]::GetFullPath((Resolve-Path $DataFile).Path)
$dataDir = Split-Path $dataFileFull -Parent
$dataFileName = Split-Path $dataFileFull -Leaf

$runnerParams = @{
    DataDir = $dataDir
    NestedCustomerFile = $dataFileName
    Queries = $Queries
    Output = $Output
    Warmups = $Warmups
    Runs = $Runs
    Threads = $Threads
    ShufflePartitions = $ShufflePartitions
    DriverMemory = $DriverMemory
    DrainMode = $DrainMode
    Image = $Image
}

foreach ($conf in $SparkConf) {
    if (-not [string]::IsNullOrWhiteSpace($conf)) {
        if (-not $runnerParams.ContainsKey("SparkConf")) {
            $runnerParams.SparkConf = @()
        }
        $runnerParams.SparkConf += $conf
    }
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

if ($PullImage) {
    $runnerParams.PullImage = $true
}

if ($DryRun) {
    $runnerParams.DryRun = $true
}

if ($VerboseSparkLogs) {
    $runnerParams.VerboseSparkLogs = $true
}

if ($PrintRows) {
    $runnerParams.PrintRows = $true
}

& $runner @runnerParams

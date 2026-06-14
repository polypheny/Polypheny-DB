param(
    [string]$Url = "jdbc:duckdb:",
    [string]$DataDir = "C:\PolyData\tlc_partitioned",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\query_lists\access_model_comparison\access_model_comparison_sql.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "..\..\results\access_model_comparison\duckdb_results.csv"),
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$FetchSize = 1000,
    [int]$QueryTimeout = 0,
    [int]$Threads = 8,
    [string]$MemoryLimit = "16GB",
    [string]$NestedCustomerFile = "",
    [string]$Only = "",
    [string]$Sql = "",
    [string]$SqlFile = "",
    [string]$DuckDbVersion = "1.5.2.1",
    [switch]$PrintRows
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\.."
$source = Join-Path $root "plugins\parquet-adapter\benchmarks\scripts\implementation\DuckDbJdbcBenchmark.java"
$classDir = Join-Path $root "build\duckdb-benchmark-client"
New-Item -ItemType Directory -Force -Path $classDir | Out-Null

if ($DuckDbVersion -eq "latest") {
    $metadata = [xml](Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/maven-metadata.xml" -UseBasicParsing).Content
    $DuckDbVersion = $metadata.metadata.versioning.release
}

$dependencyRoot = Join-Path $root "build\benchmark-deps\duckdb_jdbc\$DuckDbVersion"
$driver = Join-Path $dependencyRoot "duckdb_jdbc-$DuckDbVersion.jar"

if (-not (Test-Path $driver)) {
    New-Item -ItemType Directory -Force -Path $dependencyRoot | Out-Null
    $downloadUrl = "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/$DuckDbVersion/duckdb_jdbc-$DuckDbVersion.jar"
    Write-Host "Downloading DuckDB JDBC $DuckDbVersion from $downloadUrl"
    Invoke-WebRequest -Uri $downloadUrl -OutFile $driver
}

javac -cp $driver -d $classDir $source
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$argsList = @(
    "--url", $Url,
    "--data-dir", $DataDir,
    "--queries", $Queries,
    "--output", $Output,
    "--warmups", "$Warmups",
    "--runs", "$Runs",
    "--fetch-size", "$FetchSize",
    "--query-timeout", "$QueryTimeout",
    "--threads", "$Threads",
    "--memory-limit", $MemoryLimit
)

if ($NestedCustomerFile) {
    $argsList += @("--nested-customer-file", $NestedCustomerFile)
}

if ($Only) {
    $argsList += @("--only", $Only)
}

if ($Sql) {
    $argsList += @("--sql", $Sql)
}

if ($SqlFile) {
    $argsList += @("--sql-file", $SqlFile)
}

if ($PrintRows) {
    $argsList += "--print-rows"
}

java -cp "$classDir;$driver" DuckDbJdbcBenchmark @argsList

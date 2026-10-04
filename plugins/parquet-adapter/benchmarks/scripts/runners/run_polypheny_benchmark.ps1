param(
    [string]$Url = "jdbc:polypheny://pa:@localhost:20590/public",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\query_lists\access_model_comparison\access_model_comparison_rf.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "..\..\results\access_model_comparison\polypheny_results.csv"),
    [string]$ResultValuesOutput = "",
    [int]$Warmups = 1,
    [int]$Runs = 5,
    [int]$FetchSize = 1000,
    [int]$QueryTimeout = 0,
    [string]$TablePrefix = "tlc__",
    [string]$Only = "",
    [string]$Sql = "",
    [string]$SqlFile = "",
    [switch]$PrintRows,
    [switch]$NoTableNameMapping
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\.."
$source = Join-Path $root "plugins\parquet-adapter\benchmarks\scripts\implementation\PolyphenyJdbcBenchmark.java"
$classDir = Join-Path $root "build\parquet-benchmark-client"
New-Item -ItemType Directory -Force -Path $classDir | Out-Null

$workspaceDriver = Join-Path $root "build\benchmark-deps\polypheny_jdbc_driver\2.2\polypheny-jdbc-driver-2.2.jar"
$driver = $null

if (Test-Path $workspaceDriver) {
    $driver = Get-Item $workspaceDriver
} else {
    $gradleDriver = Get-ChildItem -Recurse -File "$env:USERPROFILE\.gradle\caches\modules-2\files-2.1\org.polypheny\polypheny-jdbc-driver\2.2" -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -eq "polypheny-jdbc-driver-2.2.jar" } |
        Select-Object -First 1

    if ($gradleDriver) {
        New-Item -ItemType Directory -Force -Path (Split-Path $workspaceDriver) | Out-Null
        Copy-Item -LiteralPath $gradleDriver.FullName -Destination $workspaceDriver -Force
        $driver = Get-Item $workspaceDriver
    }
}

if (-not $driver) {
    throw "Could not find polypheny-jdbc-driver-2.2.jar in the Gradle cache or build\benchmark-deps."
}

javac -cp $driver.FullName -d $classDir $source
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$argsList = @(
    "--url", $Url,
    "--queries", $Queries,
    "--output", $Output,
    "--warmups", "$Warmups",
    "--runs", "$Runs",
    "--fetch-size", "$FetchSize",
    "--query-timeout", "$QueryTimeout",
    "--polypheny-table-prefix", $TablePrefix,
    "--polypheny-table-names=$(-not $NoTableNameMapping)"
)

if ($Only) {
    $argsList += @("--only", $Only)
}

if ($Sql) {
    $argsList += @("--sql", $Sql)
}

if ($SqlFile) {
    $argsList += @("--sql-file", $SqlFile)
}

if ($ResultValuesOutput) {
    $argsList += @("--result-values-output", $ResultValuesOutput)
}

if ($PrintRows) {
    $argsList += "--print-rows"
}

java -cp "$classDir;$($driver.FullName)" PolyphenyJdbcBenchmark @argsList

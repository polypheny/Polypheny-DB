param(
    [string]$Server = "localhost",
    [int]$Port = 20590,
    [string]$Username = "pa",
    [string]$Password = "",
    [string]$Namespace = "pd_document",
    [string]$Language = "mongo",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\query_lists\access_model_comparison\access_model_comparison_mql.mql"),
    [string]$Output = (Join-Path $PSScriptRoot "..\..\results\access_model_comparison\polypheny_mql_results.csv"),
    [string]$ResultValuesOutput = "",
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

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\.."
$source = Join-Path $root "plugins\parquet-adapter\benchmarks\scripts\implementation\PolyphenyMqlBenchmark.java"
$classDir = Join-Path $root "build\parquet-mql-benchmark-client"
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
    "--host", $Server,
    "--port", "$Port",
    "--username", $Username,
    "--password=$Password",
    "--namespace", $Namespace,
    "--language", $Language,
    "--api-major", "$ApiMajor",
    "--api-minor", "$ApiMinor",
    "--queries", $Queries,
    "--output", $Output,
    "--warmups", "$Warmups",
    "--runs", "$Runs",
    "--fetch-size", "$FetchSize"
)

if ($Only) {
    $argsList += @("--only", $Only)
}

if ($Mql) {
    $argsList += @("--mql", $Mql)
}

if ($MqlFile) {
    $argsList += @("--mql-file", $MqlFile)
}

if ($ResultValuesOutput) {
    $argsList += @("--result-values-output", $ResultValuesOutput)
}

if ($PrintRows) {
    $argsList += "--print-rows"
}

java -cp "$classDir;$($driver.FullName)" PolyphenyMqlBenchmark @argsList

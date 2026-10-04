param(
    [string]$InputDir = "C:\PolyData\tlc_partitioned",
    [string]$OutputDir = "C:\PolyData\tlc_unpartitioned",
    [int]$Threads = 8,
    [string]$MemoryLimit = "16GB",
    [string]$Compression = "SNAPPY",
    [string]$DuckDbVersion = "1.5.2.1",
    [switch]$Overwrite,
    [switch]$DryRun,
    [switch]$VerifyOnly
)

$ErrorActionPreference = "Stop"

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\.."
$source = Join-Path $PSScriptRoot "TlcUnpartitionedMaterializer.java"
$classDir = Join-Path $root "build\tlc-unpartitioned-materializer"
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

$argsList = @(
    "--input-dir", $InputDir,
    "--output-dir", $OutputDir,
    "--threads", "$Threads",
    "--memory-limit", $MemoryLimit,
    "--compression", $Compression
)

if ($Overwrite) {
    $argsList += "--overwrite"
}

if ($DryRun) {
    $argsList += "--dry-run"
}

if ($VerifyOnly) {
    $argsList += "--verify-only"
}

java -cp "$classDir;$driver" TlcUnpartitionedMaterializer @argsList

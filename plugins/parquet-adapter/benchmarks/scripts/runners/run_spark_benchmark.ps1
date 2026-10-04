param(
    [string]$DataDir = "C:\PolyData\tlc_partitioned",
    [string]$NestedCustomerFile = "",
    [string]$Queries = (Join-Path $PSScriptRoot "..\..\query_lists\access_model_comparison\access_model_comparison_sql.sql"),
    [string]$Output = (Join-Path $PSScriptRoot "..\..\results\access_model_comparison\spark_results.csv"),
    [string]$ResultValuesOutput = "",
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

$root = Resolve-Path "$PSScriptRoot\..\..\..\..\.."
$rootFull = [System.IO.Path]::GetFullPath($root.Path).TrimEnd('\', '/')
$source = Join-Path $rootFull "plugins\parquet-adapter\benchmarks\scripts\implementation\SparkSqlBenchmark.py"

if (-not (Test-Path $source)) {
    throw "Could not find SparkSqlBenchmark.py at $source"
}

function Resolve-UnderRepo {
    param(
        [string]$PathValue,
        [switch]$MustExist
    )

    if ([string]::IsNullOrWhiteSpace($PathValue)) {
        return $null
    }

    $candidate = if ([System.IO.Path]::IsPathRooted($PathValue)) {
        $PathValue
    } else {
        Join-Path $rootFull $PathValue
    }
    $full = [System.IO.Path]::GetFullPath($candidate)

    if ($MustExist -and -not (Test-Path $full)) {
        throw "Path does not exist: $full"
    }
    if (-not $full.StartsWith($rootFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Path must be inside the repository because only the repo is mounted into the Spark container: $full"
    }
    return $full
}

function Convert-ToContainerRepoPath {
    param([string]$HostPath)

    $relative = $HostPath.Substring($rootFull.Length).TrimStart('\', '/')
    if ([string]::IsNullOrWhiteSpace($relative)) {
        return "/repo"
    }
    return "/repo/" + ($relative -replace '\\', '/')
}

$queriesHost = Resolve-UnderRepo $Queries -MustExist
$outputHost = Resolve-UnderRepo $Output
$outputParent = Split-Path $outputHost -Parent
if ($outputParent) {
    New-Item -ItemType Directory -Force -Path $outputParent | Out-Null
}

$resultValuesOutputContainer = ""
if ($ResultValuesOutput) {
    $resultValuesOutputHost = Resolve-UnderRepo $ResultValuesOutput
    $resultValuesOutputParent = Split-Path $resultValuesOutputHost -Parent
    if ($resultValuesOutputParent) {
        New-Item -ItemType Directory -Force -Path $resultValuesOutputParent | Out-Null
    }
    $resultValuesOutputContainer = Convert-ToContainerRepoPath $resultValuesOutputHost
}

$sqlFileContainer = ""
if ($SqlFile) {
    $sqlFileHost = Resolve-UnderRepo $SqlFile -MustExist
    $sqlFileContainer = Convert-ToContainerRepoPath $sqlFileHost
}

if (-not (Test-Path $DataDir)) {
    throw "Data directory does not exist: $DataDir"
}
$dataDirFull = [System.IO.Path]::GetFullPath((Resolve-Path $DataDir).Path).TrimEnd('\', '/')

$nestedCustomerContainer = ""
if ($NestedCustomerFile) {
    $nestedCustomerHost = if ([System.IO.Path]::IsPathRooted($NestedCustomerFile)) {
        [System.IO.Path]::GetFullPath($NestedCustomerFile)
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $dataDirFull $NestedCustomerFile))
    }
    if (-not (Test-Path $nestedCustomerHost)) {
        throw "Nested customer Parquet file does not exist: $nestedCustomerHost"
    }
    if (-not $nestedCustomerHost.StartsWith($dataDirFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Nested customer file must be inside DataDir because only DataDir is mounted into the Spark container: $nestedCustomerHost"
    }
    $nestedRelative = $nestedCustomerHost.Substring($dataDirFull.Length).TrimStart('\', '/')
    $nestedCustomerContainer = "/data/" + ($nestedRelative -replace '\\', '/')
}

if ($PullImage) {
    docker pull $Image
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$sparkSubmitArgs = @(
    "--master", "local[$Threads]",
    "--driver-memory", $DriverMemory,
    "--conf", "spark.sql.shuffle.partitions=$ShufflePartitions",
    "--conf", "spark.sql.session.timeZone=UTC",
    "--conf", "spark.sql.sources.partitionColumnTypeInference.enabled=false"
)

foreach ($conf in $SparkConf) {
    if (-not [string]::IsNullOrWhiteSpace($conf)) {
        $sparkSubmitArgs += @("--conf", $conf)
    }
}

$sourceContainer = Convert-ToContainerRepoPath $source
$queriesContainer = Convert-ToContainerRepoPath $queriesHost
$outputContainer = Convert-ToContainerRepoPath $outputHost
$consolePrefix = "__SPARK_BENCHMARK__ "

$benchmarkArgs = @(
    $sourceContainer,
    "--data-dir", "/data",
    "--queries", $queriesContainer,
    "--output", $outputContainer,
    "--warmups", "$Warmups",
    "--runs", "$Runs",
    "--shuffle-partitions", "$ShufflePartitions",
    "--drain-mode", $DrainMode,
    "--console-prefix", $consolePrefix
)

if ($nestedCustomerContainer) {
    $benchmarkArgs += @("--nested-customer-file", $nestedCustomerContainer)
}

if ($Only) {
    $benchmarkArgs += @("--only", $Only)
}

if ($Sql) {
    $benchmarkArgs += @("--sql", $Sql)
}

if ($sqlFileContainer) {
    $benchmarkArgs += @("--sql-file", $sqlFileContainer)
}

if ($resultValuesOutputContainer) {
    $benchmarkArgs += @("--result-values-output", $resultValuesOutputContainer)
}

if ($PrintRows) {
    $benchmarkArgs += "--print-rows"
}

$dockerArgs = @(
    "run",
    "--rm",
    "-v", "${rootFull}:/repo",
    "-v", "${dataDirFull}:/data:ro",
    "-w", "/repo",
    "-e", "SPARK_DRIVER_BIND_ADDRESS=127.0.0.1",
    $Image,
    "driver"
) + $sparkSubmitArgs + $benchmarkArgs

if ($DryRun) {
    $printable = $dockerArgs | ForEach-Object {
        if ($_ -match '\s') {
            '"' + ($_ -replace '"', '\"') + '"'
        } else {
            $_
        }
    }
    Write-Host ("docker " + ($printable -join " "))
    exit 0
}

if ($VerboseSparkLogs) {
    docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} else {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        docker @dockerArgs 2>&1 | ForEach-Object {
            $line = $_.ToString()
            if ($line.StartsWith($consolePrefix, [System.StringComparison]::Ordinal)) {
                Write-Host $line.Substring($consolePrefix.Length)
            }
        }
        $exitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    if ($exitCode -ne 0) {
        Write-Error "Spark benchmark failed with exit code $exitCode. Re-run with -VerboseSparkLogs to see the full Spark/Docker log."
        exit $exitCode
    }
}

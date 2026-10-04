param(
    [string]$InputDir = "C:\PolyData\tlc_unpartitioned",
    [string]$OutputDir = "C:\PolyData\tlc_repartitioned",
    [switch]$Copy,
    [switch]$Overwrite,
    [switch]$DryRun,
    [switch]$VerifyOnly
)

$ErrorActionPreference = "Stop"

$inputRoot = (Resolve-Path -LiteralPath $InputDir).Path
$outputRoot = [System.IO.Path]::GetFullPath($OutputDir)
$filePattern = '^(?<table>.+_tripdata)_(?<year>\d{4})-(?<month>\d{2})\.parquet$'

if ($inputRoot -eq $outputRoot) {
    throw "Input and output directories must be different."
}
if ($outputRoot.StartsWith("$inputRoot\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Output directory must not be nested under the input directory."
}

$sourceFiles = @(
    Get-ChildItem -LiteralPath $inputRoot -Recurse -File -Filter "*.parquet" |
        Sort-Object FullName
)
if ($sourceFiles.Count -eq 0) {
    throw "No parquet files found in $inputRoot."
}

$planned = foreach ($sourceFile in $sourceFiles) {
    $relative = $sourceFile.FullName.Substring($inputRoot.Length).TrimStart([char[]]"\/")
    $parts = $relative -split '[\\/]'
    if ($parts.Count -ne 2) {
        throw "Expected <table>\<file>.parquet but found: $relative"
    }

    $match = [regex]::Match($sourceFile.Name, $filePattern)
    if (-not $match.Success) {
        throw "Expected <table>_YYYY-MM.parquet but found: $relative"
    }
    $table = $match.Groups["table"].Value
    if ($parts[0] -ne $table) {
        throw "File name table '$table' does not match parent folder '$($parts[0])': $relative"
    }

    $year = $match.Groups["year"].Value
    $month = $match.Groups["month"].Value
    $outputFile = Join-Path $outputRoot "$table\year=$year\month=$month\$($sourceFile.Name)"
    [pscustomobject]@{
        Source = $sourceFile.FullName
        Output = $outputFile
        Table = $table
        Year = $year
        Month = $month
        Length = $sourceFile.Length
    }
}

Write-Host "Input:  $inputRoot"
Write-Host "Output: $outputRoot"
Write-Host "Files:  $($planned.Count)"
Write-Host "Mode:   $(if ($VerifyOnly) { "verify" } elseif ($DryRun) { "dry-run" } elseif ($Copy) { "copy" } else { "hard-link" })"
Write-Host ""

$processed = 0
$skipped = 0
foreach ($file in $planned) {
    if ($VerifyOnly) {
        if (-not (Test-Path -LiteralPath $file.Output -PathType Leaf)) {
            throw "Missing output file: $($file.Output)"
        }
        $outputFile = Get-Item -LiteralPath $file.Output
        if ($outputFile.Length -ne $file.Length) {
            throw "File length mismatch: $($file.Output)"
        }
        Write-Host "verified $($file.Output)"
        $processed++
        continue
    }

    if (Test-Path -LiteralPath $file.Output -PathType Leaf) {
        $outputFile = Get-Item -LiteralPath $file.Output
        if (-not $Overwrite -and $outputFile.Length -eq $file.Length) {
            Write-Host "skip existing $($file.Output)"
            $skipped++
            continue
        }
        if (-not $Overwrite) {
            throw "Output file already exists with a different length. Remove it or use -Overwrite: $($file.Output)"
        }
    }

    Write-Host "$($file.Table) year=$($file.Year) month=$($file.Month) -> $($file.Output)"
    if ($DryRun) {
        $processed++
        continue
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $file.Output) | Out-Null
    if ($Overwrite) {
        Remove-Item -LiteralPath $file.Output -Force -ErrorAction SilentlyContinue
    }
    if ($Copy) {
        Copy-Item -LiteralPath $file.Source -Destination $file.Output
    } else {
        New-Item -ItemType HardLink -Path $file.Output -Target $file.Source | Out-Null
    }
    $processed++
}

Write-Host ""
Write-Host "Done. Processed=$processed skipped=$skipped total=$($planned.Count)"

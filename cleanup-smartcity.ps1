<#
.SYNOPSIS
  Safely clean up a smartcity project folder by moving development junk to a timestamped backup folder.

.USAGE
  # Dry-run (default) - shows what will be moved but doesn't move:
  .\cleanup-smartcity.ps1

  # Actually perform the cleanup:
  .\cleanup-smartcity.ps1 -Confirm

  # Force cleanup without asking:
  .\cleanup-smartcity.ps1 -Confirm -Force
#>

param(
    [switch]$Confirm,
    [switch]$Force
)

# ---- Files in project root to remove ----
$rootRemoveFiles = @(
    'bengaluru_timeseries.csv',
    'csv_to_json.py',
    'db_match_raw.json',
    'dd_simplify.py',
    'dd_values.json',
    'dq_single.json',
    'dynamo_query_input.json',
    'exp.json',
    'export_to_csv.py',
    'fetchcity-policy.json',
    'fetchcity-s3-policy.json',
    'function.zip',
    'latest_processed.json',
    'names.json',
    'output.json',
    'processed_merged.csv',
    'query_result.json',
    'raw_scan.json',
    'response.json',
    'sample_items.json',
    'simplified_scan.json',
    't.json',
    'targets.json',
    'template.yaml'
)

# ---- Folders in project root to remove ----
$rootRemoveFolders = @(
    '__pycache__'
)

# ---- Items inside lambdas/fetch_city_data ----
$lambdasToClean = @{
    'lambdas\fetch_city_data' = @(
        '__pycache__',
        'build',
        'build_zip.py',
        'db_match_raw.json',
        'function.zip',
        'latest.json',
        'proc_latest_processed.json',
        'query_result.json',
        'response.json',
        't.json',
        'fetch_city_data_lambda.zip'
    )
}

Write-Host "Project root: $(Get-Location)" -ForegroundColor Cyan

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupRoot = Join-Path (Get-Location) "backup_cleanup_$timestamp"
Write-Host "Backup folder will be: $backupRoot" -ForegroundColor Cyan

$ops = @()

# Root files
foreach ($f in $rootRemoveFiles) {
    $full = Join-Path (Get-Location) $f
    if (Test-Path $full) {
        $ops += [PSCustomObject]@{
            Source      = $full
            Destination = Join-Path $backupRoot $f
        }
    }
}

# Root folders
foreach ($d in $rootRemoveFolders) {
    $full = Join-Path (Get-Location) $d
    if (Test-Path $full) {
        $ops += [PSCustomObject]@{
            Source      = $full
            Destination = Join-Path $backupRoot $d
        }
    }
}

# Lambda folder cleanup
foreach ($entry in $lambdasToClean.GetEnumerator()) {
    $base = Join-Path (Get-Location) $entry.Key
    foreach ($item in $entry.Value) {
        $full = Join-Path $base $item
        if (Test-Path $full) {
            $dest = Join-Path $backupRoot (Join-Path $entry.Key $item)
            $ops += [PSCustomObject]@{
                Source      = $full
                Destination = $dest
            }
        }
    }
}

if ($ops.Count -eq 0) {
    Write-Host "Nothing to move." -ForegroundColor Yellow
    return
}

Write-Host "`n=== DRY RUN: Items that WOULD BE MOVED ===" -ForegroundColor Green
$ops | ForEach-Object {
    Write-Host "MOVE: '$($_.Source)'  =>  '$($_.Destination)'"
}
Write-Host "Total items: $($ops.Count)`n"

if (-not $Confirm) {
    Write-Host "This was a DRY RUN. Re-run with -Confirm to perform cleanup." -ForegroundColor Yellow
    return
}

if (-not $Force) {
    $go = Read-Host "Type YES to proceed with moving files"
    if ($go -ne 'YES') {
        Write-Host "Aborted." -ForegroundColor Red
        return
    }
}

New-Item -ItemType Directory -Path $backupRoot -Force | Out-Null

foreach ($op in $ops) {
    $destDir = Split-Path $op.Destination -Parent
    if (-not (Test-Path $destDir)) {
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    }
    Move-Item -Path $op.Source -Destination $op.Destination -Force
    Write-Host "Moved: $($op.Source)" -ForegroundColor Green
}

Write-Host "`nCleanup complete! Files were moved to $backupRoot" -ForegroundColor Cyan

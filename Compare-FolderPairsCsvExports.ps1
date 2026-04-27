#Requires -Version 5.1
<#
.SYNOPSIS
    Compares source and target folder inventory CSV exports.

.DESCRIPTION
    Reads two CSV files generated from Generate-FolderPairs-CSV.ps1 (one from source and one from target),
    normalizes folder paths using the provided source and target base folder URLs, and compares item counts
    folder-by-folder.

    The script reports:
    - Folders missing in source
    - Folders missing in target
    - Source and target item counts
    - Count difference
    - Status (Matched or Mismatched)

    Output files:
    - ComparisonResults_<timestamp>.csv
    - MissingInSource_<timestamp>.csv
    - MissingInTarget_<timestamp>.csv
    - Summary_<timestamp>.json

.PARAMETER SourceCsvPath
    Path to source export CSV file.

.PARAMETER TargetCsvPath
    Path to target export CSV file.

.PARAMETER SourceFolderUrl
    Base source folder URL used when extracting source CSV data.

.PARAMETER TargetFolderUrl
    Base target folder URL used when extracting target CSV data.

.PARAMETER OutputFolder
    Folder path for output files.
    Default: ./CompareFolderPairsLog-{timestamp}

.EXAMPLE
    .\Compare-FolderPairsCsvExports.ps1 `
        -SourceCsvPath ".\Source-FolderPairs.csv" `
        -TargetCsvPath ".\Target-FolderPairs.csv" `
        -SourceFolderUrl "https://sp2019.contoso.local/sites/Finance/Shared Documents" `
        -TargetFolderUrl "https://tenant.sharepoint.com/sites/Finance/Shared Documents" `
        -OutputFolder ".\CompareFolderPairsLog"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SourceCsvPath,

    [Parameter(Mandatory = $true)]
    [string]$TargetCsvPath,

    [Parameter(Mandatory = $true)]
    [string]$SourceFolderUrl,

    [Parameter(Mandatory = $true)]
    [string]$TargetFolderUrl,

    [string]$OutputFolder = "./CompareFolderPairsLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:Results = @()
$script:ProcessedRows = 0
$script:SuccessfulRows = 0
$script:FailedRows = 0
$script:MatchedRows = 0
$script:MismatchedRows = 0
$script:MissingInSourceRows = 0
$script:MissingInTargetRows = 0
$script:SourceDuplicateRows = 0
$script:TargetDuplicateRows = 0

if (-not (Test-Path $SourceCsvPath)) {
    throw "Source CSV file not found: $SourceCsvPath"
}

if (-not (Test-Path $TargetCsvPath)) {
    throw "Target CSV file not found: $TargetCsvPath"
}

if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
    Write-Host "Created output folder: $OutputFolder"
}

# ==========================================
# Function: Get Column Name
# ==========================================
function Get-ColumnName {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$AvailableColumns,

        [Parameter(Mandatory = $true)]
        [string[]]$Candidates,

        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $column = $null

    foreach ($candidate in $Candidates) {
        if ($AvailableColumns -contains $candidate) {
            $column = $candidate
            break
        }
    }

    if ([string]::IsNullOrWhiteSpace($column)) {
        throw "Unable to find $Label column. Expected one of: $($Candidates -join ', ')"
    }

    return $column
}

# ==========================================
# Function: Normalize URL
# ==========================================
function Normalize-AbsoluteUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    $uri = $null

    if (-not [System.Uri]::TryCreate($Url, [System.UriKind]::Absolute, [ref]$uri)) {
        throw "Invalid absolute URL: $Url"
    }

    $decodedPath = [System.Uri]::UnescapeDataString($uri.AbsolutePath)
    $normalizedPath = $decodedPath.TrimEnd('/')

    if ([string]::IsNullOrWhiteSpace($normalizedPath)) {
        $normalizedPath = "/"
    }

    return ($uri.Scheme.ToLowerInvariant() + "://" + $uri.Host.ToLowerInvariant() + $normalizedPath)
}

# ==========================================
# Function: Normalize Relative Path
# ==========================================
function Normalize-RelativePath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $normalized = $Path.Trim().Replace("\", "/").Trim('/')
    while ($normalized.Contains("//")) {
        $normalized = $normalized.Replace("//", "/")
    }

    if ($normalized -eq "/") {
        return ""
    }

    return $normalized
}

# ==========================================
# Function: Get Relative Path from URL
# ==========================================
function Get-RelativePathFromUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$BaseFolderUrl
    )

    $normalizedFolderUrl = ""
    $normalizedBaseUrl = ""
    $relativePart = ""

    $normalizedFolderUrl = Normalize-AbsoluteUrl -Url $FolderUrl
    $normalizedBaseUrl = Normalize-AbsoluteUrl -Url $BaseFolderUrl

    if ($normalizedFolderUrl.Equals($normalizedBaseUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        return ""
    }

    if ($normalizedFolderUrl.StartsWith($normalizedBaseUrl + "/", [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePart = $normalizedFolderUrl.Substring($normalizedBaseUrl.Length + 1)
        return (Normalize-RelativePath -Path $relativePart)
    }

    throw "Folder URL '$FolderUrl' is not under base folder URL '$BaseFolderUrl'."
}

# ==========================================
# Function: Validate CSV Rows
# ==========================================
function Test-CsvFormat {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$Rows,

        [Parameter(Mandatory = $true)]
        [string]$CsvLabel
    )

    if ($Rows.Count -eq 0) {
        throw "$CsvLabel CSV is empty."
    }

    $first = $Rows[0]
    $columns = @($first.PSObject.Properties.Name)

    $urlColumn = Get-ColumnName -AvailableColumns $columns -Candidates @("TargetFolderUrl", "FolderUrl", "SourceFolderUrl", "Url") -Label "$CsvLabel URL"
    $countColumn = Get-ColumnName -AvailableColumns $columns -Candidates @("ItemsCount", "TotalItemsCount") -Label "$CsvLabel item count"

    return [PSCustomObject]@{
        UrlColumn = $urlColumn
        CountColumn = $countColumn
    }
}

# ==========================================
# Function: Build Folder Map
# ==========================================
function Get-FolderMapFromCsv {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$Rows,

        [Parameter(Mandatory = $true)]
        [string]$UrlColumn,

        [Parameter(Mandatory = $true)]
        [string]$CountColumn,

        [Parameter(Mandatory = $true)]
        [string]$BaseFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$CsvLabel
    )

    $map = @{}
    $rowIndex = 0

    foreach ($row in $Rows) {
        $rowIndex++

        $folderUrl = [string]$row.$UrlColumn
        $rawCount = [string]$row.$CountColumn
        $parsedCount = 0
        $relativePath = ""
        $normalizedKey = ""

        if ([string]::IsNullOrWhiteSpace($folderUrl)) {
            throw "$CsvLabel row $rowIndex has empty '$UrlColumn'."
        }

        if ([string]::IsNullOrWhiteSpace($rawCount)) {
            throw "$CsvLabel row $rowIndex has empty '$CountColumn'."
        }

        if (-not [int]::TryParse($rawCount, [ref]$parsedCount)) {
            throw "$CsvLabel row $rowIndex has invalid integer '$CountColumn': $rawCount"
        }

        if ($parsedCount -lt 0) {
            throw "$CsvLabel row $rowIndex has negative '$CountColumn': $parsedCount"
        }

        $relativePath = Get-RelativePathFromUrl -FolderUrl $folderUrl -BaseFolderUrl $BaseFolderUrl
        $normalizedKey = $relativePath.ToLowerInvariant()

        if ($map.ContainsKey($normalizedKey)) {
            $existing = $map[$normalizedKey]
            $existing.ItemsCount = [int]$existing.ItemsCount + $parsedCount
            $existing.SourceRows = [int]$existing.SourceRows + 1
        }
        else {
            $map[$normalizedKey] = [PSCustomObject]@{
                RelativePath = $relativePath
                ItemsCount = $parsedCount
                SourceRows = 1
            }
        }
    }

    return $map
}

# ==========================================
# Function: Main Processing Function
# ==========================================
function Invoke-MainFunction {
    [CmdletBinding()]
    param()

    $sourceRows = @()
    $targetRows = @()
    $sourceMeta = $null
    $targetMeta = $null
    $sourceMap = @{}
    $targetMap = @{}
    $allKeys = @()

    Write-Host "Reading source CSV: $SourceCsvPath"
    $sourceRows = Import-Csv -Path $SourceCsvPath -Encoding UTF8

    Write-Host "Reading target CSV: $TargetCsvPath"
    $targetRows = Import-Csv -Path $TargetCsvPath -Encoding UTF8

    $sourceMeta = Test-CsvFormat -Rows $sourceRows -CsvLabel "Source"
    $targetMeta = Test-CsvFormat -Rows $targetRows -CsvLabel "Target"

    Write-Host "Source URL column: $($sourceMeta.UrlColumn); Source count column: $($sourceMeta.CountColumn)"
    Write-Host "Target URL column: $($targetMeta.UrlColumn); Target count column: $($targetMeta.CountColumn)"

    $sourceMap = Get-FolderMapFromCsv -Rows $sourceRows -UrlColumn $sourceMeta.UrlColumn -CountColumn $sourceMeta.CountColumn -BaseFolderUrl $SourceFolderUrl -CsvLabel "Source"
    $targetMap = Get-FolderMapFromCsv -Rows $targetRows -UrlColumn $targetMeta.UrlColumn -CountColumn $targetMeta.CountColumn -BaseFolderUrl $TargetFolderUrl -CsvLabel "Target"

    foreach ($entry in $sourceMap.GetEnumerator()) {
        if ($entry.Value.SourceRows -gt 1) {
            $script:SourceDuplicateRows++
        }
    }

    foreach ($entry in $targetMap.GetEnumerator()) {
        if ($entry.Value.SourceRows -gt 1) {
            $script:TargetDuplicateRows++
        }
    }

    $allKeys = @($sourceMap.Keys + $targetMap.Keys | Sort-Object -Unique)

    foreach ($key in $allKeys) {
        $script:ProcessedRows++

        $inSource = $sourceMap.ContainsKey($key)
        $inTarget = $targetMap.ContainsKey($key)
        $relativePath = ""
        $sourceItems = 0
        $targetItems = 0
        $difference = 0
        $status = ""
        $isSuccessful = $true
        $message = ""

        if ($inSource) {
            $relativePath = [string]$sourceMap[$key].RelativePath
            $sourceItems = [int]$sourceMap[$key].ItemsCount
        }

        if ($inTarget) {
            if ([string]::IsNullOrWhiteSpace($relativePath)) {
                $relativePath = [string]$targetMap[$key].RelativePath
            }
            $targetItems = [int]$targetMap[$key].ItemsCount
        }

        $difference = $sourceItems - $targetItems

        if (-not $inSource) {
            $status = "Mismatched"
            $message = "Folder missing in source"
            $script:MissingInSourceRows++
            $script:MismatchedRows++
        }
        elseif (-not $inTarget) {
            $status = "Mismatched"
            $message = "Folder missing in target"
            $script:MissingInTargetRows++
            $script:MismatchedRows++
        }
        elseif ($difference -eq 0) {
            $status = "Matched"
            $message = "Counts match"
            $script:MatchedRows++
        }
        else {
            $status = "Mismatched"
            $message = "Counts do not match"
            $script:MismatchedRows++
        }

        if ($status -eq "Matched") {
            $script:SuccessfulRows++
        }
        else {
            $script:FailedRows++
            $isSuccessful = $false
        }

        $fullSourceFolderUrl = $SourceFolderUrl.TrimEnd('/')
        $fullTargetFolderUrl = $TargetFolderUrl.TrimEnd('/')

        if (-not [string]::IsNullOrWhiteSpace($relativePath)) {
            $fullSourceFolderUrl = $fullSourceFolderUrl + "/" + $relativePath
            $fullTargetFolderUrl = $fullTargetFolderUrl + "/" + $relativePath
        }

        $script:Results += [PSCustomObject]@{
            RelativeFolderPath = $relativePath
            SourceFolderUrl = $fullSourceFolderUrl
            TargetFolderUrl = $fullTargetFolderUrl
            MissingInSource = (-not $inSource)
            MissingInTarget = (-not $inTarget)
            SourceItemsCount = $sourceItems
            TargetItemsCount = $targetItems
            Difference = $difference
            Status = $status
            IsSuccessful = $isSuccessful
            Message = $message
            Timestamp = (Get-Date).ToString("s")
        }
    }
}

# ==========================================
# Main Execution
# ==========================================
$startTime = Get-Date
$endTime = $null
$duration = $null
$comparisonFile = ""
$missingInSourceFile = ""
$missingInTargetFile = ""
$summaryFile = ""

try {
    Write-Host "Starting folder CSV comparison..." -ForegroundColor Cyan

    Invoke-MainFunction

    $comparisonFile = Join-Path $OutputFolder "ComparisonResults_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $missingInSourceFile = Join-Path $OutputFolder "MissingInSource_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $missingInTargetFile = Join-Path $OutputFolder "MissingInTarget_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"

    $script:Results | Sort-Object RelativeFolderPath | Export-Csv -Path $comparisonFile -NoTypeInformation -Encoding UTF8 -Force

    @($script:Results | Where-Object { $_.MissingInSource -eq $true }) |
        Sort-Object RelativeFolderPath |
        Export-Csv -Path $missingInSourceFile -NoTypeInformation -Encoding UTF8 -Force

    @($script:Results | Where-Object { $_.MissingInTarget -eq $true }) |
        Sort-Object RelativeFolderPath |
        Export-Csv -Path $missingInTargetFile -NoTypeInformation -Encoding UTF8 -Force

    $endTime = Get-Date
    $duration = $endTime - $startTime

    $summary = @{
        ExecutionTime = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalComparedFolders = $script:ProcessedRows
        MatchedFolders = $script:MatchedRows
        MismatchedFolders = $script:MismatchedRows
        MissingInSourceFolders = $script:MissingInSourceRows
        MissingInTargetFolders = $script:MissingInTargetRows
        SourceDuplicateFolderRows = $script:SourceDuplicateRows
        TargetDuplicateFolderRows = $script:TargetDuplicateRows
        SuccessfulRows = $script:SuccessfulRows
        FailedRows = $script:FailedRows
        SourceCsvPath = $SourceCsvPath
        TargetCsvPath = $TargetCsvPath
        ComparisonFile = $comparisonFile
        MissingInSourceFile = $missingInSourceFile
        MissingInTargetFile = $missingInTargetFile
        TimestampStart = $startTime.ToString("s")
        TimestampEnd = $endTime.ToString("s")
    }

    $summary | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Compare Folder Pairs Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total Compared Folders:   $($summary.TotalComparedFolders)"
    Write-Host "Matched Folders:          $($summary.MatchedFolders)" -ForegroundColor Green
    Write-Host "Mismatched Folders:       $($summary.MismatchedFolders)" -ForegroundColor $(if ($summary.MismatchedFolders -gt 0) { "Red" } else { "Green" })
    Write-Host "Missing In Source:        $($summary.MissingInSourceFolders)" -ForegroundColor $(if ($summary.MissingInSourceFolders -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Missing In Target:        $($summary.MissingInTargetFolders)" -ForegroundColor $(if ($summary.MissingInTargetFolders -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Comparison CSV:           $comparisonFile"
    Write-Host "Missing-In-Source CSV:    $missingInSourceFile"
    Write-Host "Missing-In-Target CSV:    $missingInTargetFile"
    Write-Host "Summary JSON:             $summaryFile"
    Write-Host "Execution Time:           $($summary.ExecutionTime)"
    Write-Host "==========================================" -ForegroundColor Cyan
}
catch {
    Write-Error "Fatal error: $_"
    exit 1
}

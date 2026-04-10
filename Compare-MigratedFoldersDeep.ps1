#Requires -Version 5.1
<#
.SYNOPSIS
    Recursively compares ALL items in migrated folders between SharePoint 2019 (source)
    and SharePoint Online (target).

.DESCRIPTION
    Validates migration completeness by recursively enumerating every file and subfolder
    at all depth levels under each input folder pair.

    For every input folder pair the script produces three outputs:

      1. FolderSummary CSV      — one row per input pair: total recursive item counts
                                  (files and folders separately) for source and target,
                                  plus counts of missing and extra items.

      2. MissingItems CSV(s)    — one row per discrepant item (missing in target or extra
                                  in target) containing: relative path within root folder,
                                  item type, last-modified date, full source URL and target
                                  URL. Files are automatically split when MaxRecordsPerFile
                                  rows are reached (default 10,000) to respect the SharePoint
                                  list view threshold and to keep output files manageable.

      3. CompareDeep .log file  — every action, warning and error written immediately as
                                  it happens (no buffering).

    Source (SP2019) data is retrieved via CSOM using the currently-logged-in Windows
    credentials (no extra password needed).
    Target (SharePoint Online) data is retrieved via PnP PowerShell with the supplied
    credentials.

    The 10,000-item list view threshold is handled transparently:
      - CSOM source  : CAML paging via CamlQuery.ListItemCollectionPosition.
      - PnP target   : Get-PnPListItem with -PageSize 500 (PnP handles all pages).

.PARAMETER CsvInputPath
    Path to the input CSV file. Two formats are supported:
      Single-column  : FolderPath  (e.g. "Shared Documents/Folder1/SubFolder")
      Two-column     : SourceFolderUrl, TargetFolderUrl (full absolute URLs)

.PARAMETER SourceSiteUrl
    SharePoint 2019 base site URL used to resolve FolderPath rows.
    Example: http://sp2019.contoso.local/sites/Finance

.PARAMETER TargetSiteUrl
    SharePoint Online site URL used to resolve FolderPath rows and for PnP connection.
    Example: https://contoso.sharepoint.com/sites/Finance

.PARAMETER ClientId
    Azure AD app client ID for PnP SharePoint Online authentication.

.PARAMETER TargetUsername
    SharePoint Online username (UPN).

.PARAMETER TargetPassword
    SharePoint Online password (plain text; consistent with project credential pattern).

.PARAMETER OutputFolder
    Destination folder for all output files (log, summary CSV, missing-items CSV files).
    Default: ./DeepCompareLog-{timestamp}

.PARAMETER MaxRecordsPerFile
    Maximum rows written to a single MissingItems CSV before a new part file is created.
    Default: 10000

.EXAMPLE
    .\Compare-MigratedFoldersDeep.ps1 `
        -CsvInputPath       ".\FolderPairs_Sample.csv" `
        -SourceSiteUrl      "http://sp2019.contoso.local/sites/Finance" `
        -TargetSiteUrl      "https://contoso.sharepoint.com/sites/Finance" `
        -ClientId           "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -TargetUsername     "admin@contoso.onmicrosoft.com" `
        -TargetPassword     "Password123!" `
        -OutputFolder       "./DeepCompareLogs" `
        -MaxRecordsPerFile  10000
#>

[Diagnostics.CodeAnalysis.SuppressMessageAttribute(
    'PSAvoidUsingPlainTextForPassword', 'TargetPassword',
    Justification = 'Plain-text password parameter is intentional; matches the established project credential pattern.')]
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$CsvInputPath,

    [Parameter(Mandatory = $true)]
    [string]$SourceSiteUrl,

    [Parameter(Mandatory = $true)]
    [string]$TargetSiteUrl,

    [Parameter(Mandatory = $true)]
    [string]$ClientId,

    [Parameter(Mandatory = $true)]
    [string]$TargetUsername,

    [Parameter(Mandatory = $true)]
    [string]$TargetPassword,

    [string]$OutputFolder = "./DeepCompareLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')",

    [int]$MaxRecordsPerFile = 10000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:MissingItems                    = @()
$script:CurrentSummaryFile              = ""   # path of the FolderSummary CSV written to immediately
$script:MissingItemsFileCounter         = 1
$script:CurrentMissingItemsFile         = ""   # path of the file currently being appended to
$script:CurrentMissingItemsFileRowCount = 0    # rows written to that file so far
$script:TotalMissingInTarget            = 0
$script:TotalExtraInTarget      = 0
$script:TotalDifferences        = 0
$script:ProcessedFolders        = 0
$script:FailedFolders           = 0
$script:SourceContexts          = @{}
$script:Credential              = $null
$script:LastTargetSiteUrl       = ""
$script:LogFile                 = ""

if (-not (Test-Path $CsvInputPath)) {
    throw "CSV input file not found: $CsvInputPath"
}

if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
    Write-Host "Created output folder: $OutputFolder"
}

$script:LogFile = Join-Path $OutputFolder "CompareDeep_$(Get-Date -Format 'yyyyMMdd-HHmmss').log"

# ==========================================
# Function: Write-Log
# ==========================================
function Write-Log {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,

        [ValidateSet("INFO", "WARN", "ERROR", "SUCCESS")]
        [string]$Level = "INFO"
    )

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $entry     = "[$timestamp] [$Level] $Message"

    # Write immediately to log file — no buffering
    Add-Content -Path $script:LogFile -Value $entry -Encoding UTF8

    switch ($Level) {
        "WARN"    { Write-Host $entry -ForegroundColor Yellow }
        "ERROR"   { Write-Host $entry -ForegroundColor Red }
        "SUCCESS" { Write-Host $entry -ForegroundColor Green }
        default   { Write-Host $entry }
    }
}

# ==========================================
# Function: Import CSOM Assemblies
# ==========================================
function Import-CSOMAssemblies {
    [CmdletBinding()]
    param()

    $candidatePaths = @(
        "C:\Program Files\Common Files\microsoft shared\Web Server Extensions\16\ISAPI",
        "C:\Program Files\Common Files\microsoft shared\Web Server Extensions\15\ISAPI"
    )

    $loaded = $false
    foreach ($path in $candidatePaths) {
        $runtimeDll = Join-Path $path "Microsoft.SharePoint.Client.Runtime.dll"
        $clientDll  = Join-Path $path "Microsoft.SharePoint.Client.dll"

        if ((Test-Path $runtimeDll) -and (Test-Path $clientDll)) {
            Add-Type -Path $runtimeDll -ErrorAction SilentlyContinue
            Add-Type -Path $clientDll  -ErrorAction SilentlyContinue
            $loaded = $true
            Write-Log "CSOM assemblies loaded from: $path"
            break
        }
    }

    if (-not $loaded) {
        throw "CSOM assemblies not found. Ensure SharePoint Server CSOM or client libraries are installed."
    }
}

# ==========================================
# Function: Get or Create Source Context
# ==========================================
function Get-SourceContext {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    $siteKey = $SiteUrl.ToLowerInvariant()

    if (-not $script:SourceContexts.ContainsKey($siteKey)) {
        try {
            $ctx = New-Object Microsoft.SharePoint.Client.ClientContext($SiteUrl)
            $ctx.Credentials   = [System.Net.CredentialCache]::DefaultNetworkCredentials
            $ctx.RequestTimeout = 300000   # 5 minutes — needed for large recursive queries
            $script:SourceContexts[$siteKey] = $ctx
            Write-Log "Source CSOM context created for: $SiteUrl"
        }
        catch {
            Write-Log "Failed to create source CSOM context for '$SiteUrl': $_" -Level ERROR
            return $null
        }
    }

    return $script:SourceContexts[$siteKey]
}

# ==========================================
# Function: Initialize Credential
# ==========================================
function Initialize-Credential {
    [CmdletBinding()]
    param()

    $securePassword    = ConvertTo-SecureString $TargetPassword -AsPlainText -Force
    $script:Credential = New-Object System.Management.Automation.PSCredential($TargetUsername, $securePassword)
}

# ==========================================
# Function: Connect to SPO Site
# ==========================================
function Get-PnPConnectionForSite {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    if ($script:LastTargetSiteUrl -eq $SiteUrl) {
        return
    }

    try {
        Write-Log "Connecting to SharePoint Online: $SiteUrl"
        Connect-PnPOnline -Url $SiteUrl -Credentials $script:Credential -ClientId $ClientId -ErrorAction Stop
        $script:LastTargetSiteUrl = $SiteUrl
        Write-Log "Connected to SPO: $SiteUrl" -Level SUCCESS
    }
    catch {
        throw "Failed to connect to target site '$SiteUrl': $_"
    }
}

# ==========================================
# Function: Validate CSV Columns
# ==========================================
function Test-CsvFormat {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$Rows
    )

    if ($Rows.Count -eq 0) {
        throw "CSV file is empty: $CsvInputPath"
    }

    $first           = $Rows[0]
    $hasFolderPath   = $first.PSObject.Properties.Name -contains "FolderPath"
    $hasSourceTarget = ($first.PSObject.Properties.Name -contains "SourceFolderUrl") -and
                       ($first.PSObject.Properties.Name -contains "TargetFolderUrl")

    if (-not $hasFolderPath -and -not $hasSourceTarget) {
        throw "CSV must contain either a 'FolderPath' column, or both 'SourceFolderUrl' and 'TargetFolderUrl' columns."
    }
}

# ==========================================
# Function: Path Helpers
# ==========================================
function Normalize-FolderPath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderPath
    )

    $trimmed = $FolderPath.Trim() -replace '\\', '/'
    return $trimmed.Trim('/')
}

function Build-FolderUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$FolderPath
    )

    $base     = $SiteUrl.TrimEnd('/')
    $relative = Normalize-FolderPath -FolderPath $FolderPath
    return "$base/$relative"
}

function Get-ServerRelativePathFromUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,

        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    $normalizedUrl     = $Url.Trim() -replace '\\', '/'
    $normalizedSiteUrl = $SiteUrl.TrimEnd('/')

    if ($normalizedUrl.StartsWith($normalizedSiteUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePart = $normalizedUrl.Substring($normalizedSiteUrl.Length).TrimStart('/')
        $siteUri      = New-Object System.Uri($normalizedSiteUrl)
        $sitePath     = $siteUri.AbsolutePath.TrimEnd('/')

        if ([string]::IsNullOrWhiteSpace($sitePath) -or $sitePath -eq '/') {
            return "/$relativePart"
        }
        return "$sitePath/$relativePart"
    }

    if ($normalizedUrl -match '^https?://[^/]+(?<path>/.*)$') {
        return $matches.path
    }

    return "/$($normalizedUrl.TrimStart('/'))"
}

function Get-SiteRelativePathFromUrl {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,

        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    $serverRelativePath = Get-ServerRelativePathFromUrl -Url $Url -SiteUrl $SiteUrl
    $siteUri            = New-Object System.Uri($SiteUrl)
    $sitePath           = $siteUri.AbsolutePath.TrimEnd('/')

    if ((-not [string]::IsNullOrWhiteSpace($sitePath)) -and ($sitePath -ne '/') -and
        $serverRelativePath.StartsWith($sitePath, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $serverRelativePath.Substring($sitePath.Length).TrimStart('/')
    }

    return $serverRelativePath.TrimStart('/')
}

function Resolve-FolderPair {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Row
    )

    if ($Row.PSObject.Properties.Name -contains "FolderPath") {
        $folderPath = [string]$Row.FolderPath
        if ([string]::IsNullOrWhiteSpace($folderPath)) {
            throw "CSV row contains empty FolderPath"
        }
        $normalized = Normalize-FolderPath -FolderPath $folderPath
        return [PSCustomObject]@{
            SourceFolderUrl = Build-FolderUrl -SiteUrl $SourceSiteUrl -FolderPath $normalized
            TargetFolderUrl = Build-FolderUrl -SiteUrl $TargetSiteUrl -FolderPath $normalized
            FolderPath      = $normalized
        }
    }

    $derivedFolderPath = ""
    try {
        $derivedFolderPath = Get-SiteRelativePathFromUrl -Url ([string]$Row.SourceFolderUrl) -SiteUrl $SourceSiteUrl
    }
    catch {
        $derivedFolderPath = [string]$Row.SourceFolderUrl
    }

    return [PSCustomObject]@{
        SourceFolderUrl = [string]$Row.SourceFolderUrl
        TargetFolderUrl = [string]$Row.TargetFolderUrl
        FolderPath      = $derivedFolderPath
    }
}

# ==========================================
# Function: Get All Items Recursively from Source (SP2019)
# ==========================================
function Get-SourceItemsRecursive {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderUrl,

        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context
    )

    $serverRelativePath = Get-ServerRelativePathFromUrl -Url $FolderUrl -SiteUrl $SourceSiteUrl

    # Derive library server-relative URL: site path + first URL segment after site path
    $siteUri           = New-Object System.Uri($SourceSiteUrl)
    $sitePath          = $siteUri.AbsolutePath.TrimEnd('/')
    $pathAfterSite     = $serverRelativePath.Substring($sitePath.Length).TrimStart('/')
    $libraryUrlName    = $pathAfterSite.Split('/')[0]
    $listSrvRelUrl     = if ([string]::IsNullOrWhiteSpace($sitePath) -or $sitePath -eq '/') {
        "/$libraryUrlName"
    }
    else {
        "$sitePath/$libraryUrlName"
    }

    Write-Log "Source: fetching list at '$listSrvRelUrl' recursively under '$serverRelativePath'"

    $list = $null
    try {
        $list = $Context.Web.GetList($listSrvRelUrl)
        $Context.Load($list)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to load source list '$listSrvRelUrl': $_"
    }

    # CAML: RecursiveAll scope, filter FileDirRef starts with root folder path.
    # Requesting only the five fields we need keeps each page response small.
    # ListItemCollectionPosition handles paging past the 5000-item CSOM server page limit.
    $escapedRootPath = [System.Security.SecurityElement]::Escape($serverRelativePath)
    $camlQuery       = New-Object Microsoft.SharePoint.Client.CamlQuery
    $camlQuery.ViewXml = "<View Scope='RecursiveAll'>" +
        "<ViewFields>" +
            "<FieldRef Name='FileLeafRef'/>" +
            "<FieldRef Name='FileRef'/>" +
            "<FieldRef Name='FileDirRef'/>" +
            "<FieldRef Name='FSObjType'/>" +
            "<FieldRef Name='Modified'/>" +
        "</ViewFields>" +
        "<Query><Where><BeginsWith>" +
            "<FieldRef Name='FileDirRef'/><Value Type='Lookup'>$escapedRootPath</Value>" +
        "</BeginsWith></Where></Query>" +
        "<RowLimit>2000</RowLimit></View>"

    $allItems  = @()
    $pageCount = 0

    do {
        $pageCount++
        $pageItems = $null
        try {
            $pageItems = $list.GetItems($camlQuery)
            $Context.Load($pageItems)
            $Context.ExecuteQuery()
        }
        catch {
            throw "Source query failed (page $pageCount) for '$serverRelativePath': $_"
        }

        Write-Log "Source: page $pageCount — $($pageItems.Count) items returned"

        foreach ($item in $pageItems) {
            $name        = [string]$item["FileLeafRef"]
            $fileRef     = [string]$item["FileRef"]
            $fsObjType   = [int]$item["FSObjType"]
            $lastModified = $item["Modified"]

            # Skip the built-in "Forms" system folder (present at library root)
            if ($fsObjType -eq 1 -and $name -eq "Forms") {
                continue
            }

            # Guard against CAML prefix ambiguity (e.g. "Folder1" vs "Folder10"):
            # only keep items whose FileRef is genuinely inside our root folder.
            if (-not $fileRef.StartsWith($serverRelativePath + "/", [System.StringComparison]::OrdinalIgnoreCase)) {
                continue
            }

            # RelativePath: path of this item within the root folder (no leading slash)
            $relativePath = $fileRef.Substring($serverRelativePath.Length).TrimStart('/')
            $itemType     = if ($fsObjType -eq 1) { "Folder" } else { "File" }

            $allItems += [PSCustomObject]@{
                Name         = $name
                RelativePath = $relativePath
                Url          = $fileRef
                Type         = $itemType
                LastModified = $lastModified
            }
        }

        $camlQuery.ListItemCollectionPosition = $pageItems.ListItemCollectionPosition

    } while ($null -ne $pageItems.ListItemCollectionPosition)

    Write-Log "Source: $($allItems.Count) items retrieved recursively from '$FolderUrl'"
    return $allItems
}

# ==========================================
# Function: Get All Items Recursively from Target (SPO)
# ==========================================
function Get-TargetItemsRecursive {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderUrl
    )

    $serverRelativePath = Get-ServerRelativePathFromUrl -Url $FolderUrl -SiteUrl $TargetSiteUrl

    # Derive library name (first URL segment after site path)
    $siteUri        = New-Object System.Uri($TargetSiteUrl)
    $sitePath       = $siteUri.AbsolutePath.TrimEnd('/')
    $pathAfterSite  = $serverRelativePath.Substring($sitePath.Length).TrimStart('/')
    $libraryUrlName = $pathAfterSite.Split('/')[0]

    Write-Log "Target: fetching list '$libraryUrlName' recursively under '$serverRelativePath'"

    # CAML: RecursiveAll, BeginsWith on FileDirRef.
    # PnP.PowerShell with -PageSize handles the 10,000-item threshold automatically —
    # it pages through all results internally without extra loop logic needed here.
    $escapedRootPath = [System.Security.SecurityElement]::Escape($serverRelativePath)
    $camlViewXml     = "<View Scope='RecursiveAll'>" +
        "<ViewFields>" +
            "<FieldRef Name='FileLeafRef'/>" +
            "<FieldRef Name='FileRef'/>" +
            "<FieldRef Name='FileDirRef'/>" +
            "<FieldRef Name='FSObjType'/>" +
            "<FieldRef Name='Modified'/>" +
        "</ViewFields>" +
        "<Query><Where><BeginsWith>" +
            "<FieldRef Name='FileDirRef'/><Value Type='Lookup'>$escapedRootPath</Value>" +
        "</BeginsWith></Where></Query></View>"

    $pnpItems = $null
    try {
        $pnpItems = Get-PnPListItem -List $libraryUrlName -Query $camlViewXml -PageSize 500 -ErrorAction Stop
    }
    catch {
        throw "Failed to retrieve target items for folder '$FolderUrl': $_"
    }

    $allItems = @()

    if ($null -eq $pnpItems) {
        Write-Log "Target: no items returned for '$FolderUrl'"
        return $allItems
    }

    foreach ($item in $pnpItems) {
        $name      = [string]$item.FieldValues["FileLeafRef"]
        $fileRef   = [string]$item.FieldValues["FileRef"]
        $fsObjType = [int]$item.FieldValues["FSObjType"]
        $modified  = $item.FieldValues["Modified"]

        # Skip built-in "Forms" folder
        if ($fsObjType -eq 1 -and $name -eq "Forms") {
            continue
        }

        # Guard against prefix ambiguity (same as source side)
        if (-not $fileRef.StartsWith($serverRelativePath + "/", [System.StringComparison]::OrdinalIgnoreCase)) {
            continue
        }

        $relativePath = $fileRef.Substring($serverRelativePath.Length).TrimStart('/')
        $itemType     = if ($fsObjType -eq 1) { "Folder" } else { "File" }

        $allItems += [PSCustomObject]@{
            Name         = $name
            RelativePath = $relativePath
            Url          = $fileRef
            Type         = $itemType
            LastModified = $modified
        }
    }

    Write-Log "Target: $($allItems.Count) items retrieved recursively from '$FolderUrl'"
    return $allItems
}

# ==========================================
# Function: Flush Missing Items Buffer to CSV (immediate, append mode)
# ==========================================
function Export-MissingItemsToFile {
    [CmdletBinding()]
    param()

    if ($script:MissingItems.Count -eq 0) {
        return
    }

    $rowsToWrite = $script:MissingItems.Count
    $written     = 0

    while ($written -lt $rowsToWrite) {
        # Open a new part file when no file is active or the current one is full
        if ([string]::IsNullOrEmpty($script:CurrentMissingItemsFile) -or
            $script:CurrentMissingItemsFileRowCount -ge $MaxRecordsPerFile) {

            $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
            $script:CurrentMissingItemsFile         = Join-Path $OutputFolder "MissingItems_Part$($script:MissingItemsFileCounter)_$timestamp.csv"
            $script:MissingItemsFileCounter++
            $script:CurrentMissingItemsFileRowCount  = 0
            Write-Log "New MissingItems file: $($script:CurrentMissingItemsFile)"
        }

        # How many rows still fit in the current file
        $capacity  = $MaxRecordsPerFile - $script:CurrentMissingItemsFileRowCount
        $batch     = @($script:MissingItems | Select-Object -Skip $written -First $capacity)
        $batchSize = $batch.Count

        try {
            if ($script:CurrentMissingItemsFileRowCount -eq 0) {
                # First write to this file — create with header
                $batch | Export-Csv -Path $script:CurrentMissingItemsFile -NoTypeInformation -Encoding UTF8 -Force
            }
            else {
                # Append — PS 5.1 Export-Csv -Append skips the header row automatically
                $batch | Export-Csv -Path $script:CurrentMissingItemsFile -NoTypeInformation -Encoding UTF8 -Append
            }
            $script:CurrentMissingItemsFileRowCount += $batchSize
            $written += $batchSize
            Write-Log "Written $batchSize records to: $($script:CurrentMissingItemsFile)" -Level SUCCESS
        }
        catch {
            Write-Log "Failed to write MissingItems CSV: $_" -Level ERROR
            break
        }
    }

    $script:MissingItems = @()
}

# ==========================================
# Function: Write Summary Row Immediately to CSV
# ==========================================
function Export-SummaryRowToFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [PSCustomObject]$Row
    )

    # Create the file path on the very first call — same file for the whole run
    if ([string]::IsNullOrEmpty($script:CurrentSummaryFile)) {
        $script:CurrentSummaryFile = Join-Path $OutputFolder "FolderSummary_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
        Write-Log "FolderSummary file: $($script:CurrentSummaryFile)"
    }

    try {
        if (Test-Path $script:CurrentSummaryFile) {
            # File already exists — append without repeating the header
            $Row | Export-Csv -Path $script:CurrentSummaryFile -NoTypeInformation -Encoding UTF8 -Append
        }
        else {
            # First row — create file with header
            $Row | Export-Csv -Path $script:CurrentSummaryFile -NoTypeInformation -Encoding UTF8 -Force
        }
    }
    catch {
        Write-Log "Failed to write FolderSummary row: $_" -Level ERROR
    }
}

# ==========================================
# Function: Add Missing/Extra Item Record
# ==========================================
function Add-MissingItemRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$RowNumber,

        [Parameter(Mandatory = $true)]
        [string]$InputFolderPath,

        [Parameter(Mandatory = $true)]
        [string]$RelativePath,

        [Parameter(Mandatory = $true)]
        [string]$ItemName,

        [Parameter(Mandatory = $true)]
        [string]$ItemType,

        [string]$SourceUrl       = "",
        [string]$TargetUrl       = "",
        $LastModifiedDate        = $null,

        [Parameter(Mandatory = $true)]
        [string]$Status
    )

    $formattedDate = ""
    if ($null -ne $LastModifiedDate) {
        try {
            $formattedDate = ([datetime]$LastModifiedDate).ToString("yyyy-MM-dd HH:mm:ss")
        }
        catch {
            $formattedDate = [string]$LastModifiedDate
        }
    }

    $script:MissingItems += [PSCustomObject]@{
        Timestamp        = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        RowNumber        = $RowNumber
        InputFolderPath  = $InputFolderPath
        RelativePath     = $RelativePath
        ItemName         = $ItemName
        ItemType         = $ItemType
        SourceUrl        = $SourceUrl
        TargetUrl        = $TargetUrl
        LastModifiedDate = $formattedDate
        Status           = $Status
    }

    if ($Status -eq "Missing in Target") {
        $script:TotalMissingInTarget++
    }
    elseif ($Status -eq "Extra in Target") {
        $script:TotalExtraInTarget++
    }
    $script:TotalDifferences++

    # Flush buffer to file once MaxRecordsPerFile is reached
    if ($script:MissingItems.Count -ge $MaxRecordsPerFile) {
        Write-Log "Missing-items buffer hit $MaxRecordsPerFile rows — flushing to file."
        Export-MissingItemsToFile
    }
}

# ==========================================
# Function: Invoke-DeepFolderComparison
# ==========================================
function Invoke-DeepFolderComparison {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$RowNumber,

        [Parameter(Mandatory = $true)]
        [string]$SourceFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$TargetFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$InputFolderPath
    )

    # Pre-declare variables used in catch blocks (Set-StrictMode requirement)
    $sourceItems    = @()
    $targetItems    = @()
    $errorMessage   = ""
    $status         = "Success"

    Write-Log "--- Row $RowNumber : $InputFolderPath ---"
    Write-Log "    Source : $SourceFolderUrl"
    Write-Log "    Target : $TargetFolderUrl"

    # --- Fetch source items ---
    try {
        $sourceContext = Get-SourceContext -SiteUrl $SourceSiteUrl
        if ($null -eq $sourceContext) {
            throw "Could not obtain source CSOM context for '$SourceSiteUrl'"
        }
        $sourceItems = Get-SourceItemsRecursive -FolderUrl $SourceFolderUrl -Context $sourceContext
    }
    catch {
        $errorMessage = "Error fetching source items: $_"
        Write-Log "Row $RowNumber | $errorMessage" -Level ERROR
        $status = "Failed"
    }

    # --- Fetch target items ---
    if ($status -ne "Failed") {
        try {
            $targetItems = Get-TargetItemsRecursive -FolderUrl $TargetFolderUrl
        }
        catch {
            $errorMessage = "Error fetching target items: $_"
            Write-Log "Row $RowNumber | $errorMessage" -Level ERROR
            $status = "Failed"
        }
    }

    # --- Count totals ---
    $sourceTotalFiles   = 0
    $sourceTotalFolders = 0
    $targetTotalFiles   = 0
    $targetTotalFolders = 0
    $missingCount       = 0
    $extraCount         = 0

    if ($status -ne "Failed") {
        foreach ($si in $sourceItems) {
            if ($si.Type -eq "File") { $sourceTotalFiles++ } else { $sourceTotalFolders++ }
        }
        foreach ($ti in $targetItems) {
            if ($ti.Type -eq "File") { $targetTotalFiles++ } else { $targetTotalFolders++ }
        }

        Write-Log ("Row $RowNumber | Source: {0} items ({1} files, {2} folders) | Target: {3} items ({4} files, {5} folders)" -f `
            $sourceItems.Count, $sourceTotalFiles, $sourceTotalFolders, `
            $targetItems.Count, $targetTotalFiles, $targetTotalFolders)

        # Build lookup tables keyed by lowercase relative path for O(1) lookups
        $targetLookup = @{}
        foreach ($ti in $targetItems) {
            $targetLookup[$ti.RelativePath.ToLowerInvariant()] = $ti
        }

        $sourceLookup = @{}
        foreach ($si in $sourceItems) {
            $sourceLookup[$si.RelativePath.ToLowerInvariant()] = $si
        }

        # Find items that exist in source but are missing from target
        foreach ($si in $sourceItems) {
            $key = $si.RelativePath.ToLowerInvariant()
            if (-not $targetLookup.ContainsKey($key)) {
                $missingCount++
                Write-Log "  MISSING in target : [$($si.Type)] $($si.RelativePath)" -Level WARN
                Add-MissingItemRecord `
                    -RowNumber        $RowNumber `
                    -InputFolderPath  $InputFolderPath `
                    -RelativePath     $si.RelativePath `
                    -ItemName         $si.Name `
                    -ItemType         $si.Type `
                    -SourceUrl        "$SourceFolderUrl/$($si.RelativePath)" `
                    -TargetUrl        "$TargetFolderUrl/$($si.RelativePath)" `
                    -LastModifiedDate  $si.LastModified `
                    -Status           "Missing in Target"
            }
        }

        # Find items that exist in target but are absent from source
        foreach ($ti in $targetItems) {
            $key = $ti.RelativePath.ToLowerInvariant()
            if (-not $sourceLookup.ContainsKey($key)) {
                $extraCount++
                Write-Log "  EXTRA in target : [$($ti.Type)] $($ti.RelativePath)" -Level WARN
                Add-MissingItemRecord `
                    -RowNumber        $RowNumber `
                    -InputFolderPath  $InputFolderPath `
                    -RelativePath     $ti.RelativePath `
                    -ItemName         $ti.Name `
                    -ItemType         $ti.Type `
                    -SourceUrl        "$SourceFolderUrl/$($ti.RelativePath)" `
                    -TargetUrl        "$TargetFolderUrl/$($ti.RelativePath)" `
                    -LastModifiedDate  $ti.LastModified `
                    -Status           "Extra in Target"
            }
        }

        $script:ProcessedFolders++

        $diffLevel = if (($missingCount + $extraCount) -gt 0) { "WARN" } else { "SUCCESS" }
        Write-Log "Row $RowNumber | Missing=$missingCount | Extra=$extraCount | Differences=$($missingCount + $extraCount)" -Level $diffLevel
    }
    else {
        $script:FailedFolders++
    }

    # Flush all missing/extra items collected for THIS folder to disk immediately
    # before processing the next folder pair.
    Export-MissingItemsToFile

    # Write this folder's summary row to the FolderSummary CSV immediately
    $summaryRow = [PSCustomObject]@{
        Timestamp          = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        RowNumber          = $RowNumber
        InputFolderPath    = $InputFolderPath
        SourceFolderUrl    = $SourceFolderUrl
        TargetFolderUrl    = $TargetFolderUrl
        SourceTotalItems   = $sourceItems.Count
        SourceTotalFiles   = $sourceTotalFiles
        SourceTotalFolders = $sourceTotalFolders
        TargetTotalItems   = $targetItems.Count
        TargetTotalFiles   = $targetTotalFiles
        TargetTotalFolders = $targetTotalFolders
        MissingInTarget    = $missingCount
        ExtraInTarget      = $extraCount
        TotalDifferences   = $missingCount + $extraCount
        Status             = $status
        ErrorMessage       = $errorMessage
    }
    Export-SummaryRowToFile -Row $summaryRow
}

# ==========================================
# Function: Invoke-Main
# ==========================================
function Invoke-Main {
    [CmdletBinding()]
    param()

    Write-Log "=========================================="
    Write-Log "Compare-MigratedFoldersDeep  started"
    Write-Log "=========================================="
    Write-Log "CsvInputPath     : $CsvInputPath"
    Write-Log "SourceSiteUrl    : $SourceSiteUrl"
    Write-Log "TargetSiteUrl    : $TargetSiteUrl"
    Write-Log "OutputFolder     : $OutputFolder"
    Write-Log "MaxRecordsPerFile: $MaxRecordsPerFile"

    Import-CSOMAssemblies
    Initialize-Credential
    Get-PnPConnectionForSite -SiteUrl $TargetSiteUrl

    # Read input CSV
    $csvRows = $null
    try {
        $csvRows = Import-Csv -Path $CsvInputPath -ErrorAction Stop
    }
    catch {
        throw "Failed to read CSV '$CsvInputPath': $_"
    }

    Test-CsvFormat -Rows $csvRows
    Write-Log "CSV loaded: $($csvRows.Count) rows"

    $startTime = Get-Date
    $rowIndex  = 0

    foreach ($row in $csvRows) {
        $rowIndex++

        $pair      = $null
        $pairError = ""

        try {
            $pair = Resolve-FolderPair -Row $row
        }
        catch {
            $pairError = "Could not resolve folder pair: $_"
            Write-Log "Row $rowIndex | Skipping — $pairError" -Level ERROR
            $script:FailedFolders++

            $failedSummaryRow = [PSCustomObject]@{
                Timestamp          = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
                RowNumber          = $rowIndex
                InputFolderPath    = ""
                SourceFolderUrl    = ""
                TargetFolderUrl    = ""
                SourceTotalItems   = 0
                SourceTotalFiles   = 0
                SourceTotalFolders = 0
                TargetTotalItems   = 0
                TargetTotalFiles   = 0
                TargetTotalFolders = 0
                MissingInTarget    = 0
                ExtraInTarget      = 0
                TotalDifferences   = 0
                Status             = "Failed"
                ErrorMessage       = $pairError
            }
            Export-SummaryRowToFile -Row $failedSummaryRow
            continue
        }

        Invoke-DeepFolderComparison `
            -RowNumber       $rowIndex `
            -SourceFolderUrl $pair.SourceFolderUrl `
            -TargetFolderUrl $pair.TargetFolderUrl `
            -InputFolderPath $pair.FolderPath

        $pct = [Math]::Min(100, [int](($rowIndex / $csvRows.Count) * 100))
        Write-Progress -Activity "Deep folder comparison" `
            -Status "Row $rowIndex of $($csvRows.Count)" `
            -CurrentOperation $pair.FolderPath `
            -PercentComplete $pct
    }

    Write-Progress -Activity "Deep folder comparison" -Completed

    # Flush any remaining buffered missing items to CSV
    if ($script:MissingItems.Count -gt 0) {
        Export-MissingItemsToFile
    }

    # ---- FolderSummary CSV ----
    # Already fully written row-by-row during processing — just confirm its location.
    Write-Log "FolderSummary CSV complete: $($script:CurrentSummaryFile)" -Level SUCCESS

    # ---- Export JSON run summary ----
    $endTime      = Get-Date
    $duration     = $endTime - $startTime
    $jsonSummary  = @{
        ExecutionTime     = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalFolderPairs  = $csvRows.Count
        ProcessedFolders  = $script:ProcessedFolders
        FailedFolders     = $script:FailedFolders
        TotalDifferences  = $script:TotalDifferences
        MissingInTarget   = $script:TotalMissingInTarget
        ExtraInTarget     = $script:TotalExtraInTarget
        MissingItemFiles  = $script:MissingItemsFileCounter - 1
        FolderSummaryFile = $script:CurrentSummaryFile
        LogFile           = $script:LogFile
        TimestampStart    = $startTime.ToString("s")
        TimestampEnd      = $endTime.ToString("s")
    }

    $jsonFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"
    try {
        $jsonSummary | ConvertTo-Json | Set-Content -Path $jsonFile -Encoding UTF8
        Write-Log "JSON summary written to: $jsonFile" -Level SUCCESS
    }
    catch {
        Write-Log "Failed to write JSON summary: $_" -Level ERROR
    }

    # ---- Console summary ----
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "  Deep Folder Comparison — Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total Folder Pairs:      $($jsonSummary.TotalFolderPairs)"
    Write-Host "Processed Successfully:  $($jsonSummary.ProcessedFolders)" -ForegroundColor Green
    Write-Host "Failed:                  $($jsonSummary.FailedFolders)"  -ForegroundColor $(if ($jsonSummary.FailedFolders  -gt 0) { "Red"    } else { "Green" })
    Write-Host "Missing In Target:       $($jsonSummary.MissingInTarget)" -ForegroundColor $(if ($jsonSummary.MissingInTarget -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Extra In Target:         $($jsonSummary.ExtraInTarget)"   -ForegroundColor $(if ($jsonSummary.ExtraInTarget   -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Total Differences:       $($jsonSummary.TotalDifferences)" -ForegroundColor $(if ($jsonSummary.TotalDifferences -gt 0) { "Yellow" } else { "Green" })
    Write-Host ""
    Write-Host "Output Folder:           $OutputFolder"
    Write-Host "Folder Summary CSV:      $($script:CurrentSummaryFile)"
    Write-Host "JSON Summary:            $jsonFile"
    Write-Host "Log File:                $($script:LogFile)"
    Write-Host "MissingItems CSV files:  $($jsonSummary.MissingItemFiles)"
    Write-Host "Execution Time:          $($jsonSummary.ExecutionTime)"
    Write-Host "==========================================" -ForegroundColor Cyan

    Write-Log "Run complete. Processed=$($jsonSummary.ProcessedFolders) Failed=$($jsonSummary.FailedFolders) Missing=$($jsonSummary.MissingInTarget) Extra=$($jsonSummary.ExtraInTarget)"
    Write-Log "=========================================="
}

# ==========================================
# Main Execution
# ==========================================
try {
    Invoke-Main
    Write-Host ""
    Write-Host "✓ Deep folder comparison completed successfully." -ForegroundColor Green
}
catch {
    if ($script:LogFile -ne "") {
        Write-Log "FATAL: $_" -Level ERROR
    }
    Write-Host "✗ Script failed: $_" -ForegroundColor Red
    exit 1
}
finally {
    Disconnect-PnPOnline -ErrorAction SilentlyContinue
    Write-Host "Script execution completed." -ForegroundColor Cyan
}

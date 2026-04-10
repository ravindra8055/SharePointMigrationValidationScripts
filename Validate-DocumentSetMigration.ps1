#Requires -Version 5.1
<#
.SYNOPSIS
    Validates document set migration from SP2016/SP2019 to SharePoint Online.

.DESCRIPTION
    Reads a CSV file with SourceSiteUrl, TargetSiteUrl, LibraryName, and NewLocation columns.
    For each row, queries the source document library (SP2016/SP2019) for all document sets
    where the "New Location" column value matches the specified NewLocation value.
    For each matching document set found in source, the script:
      1) Checks whether the document set exists in the target library (SharePoint Online).
      2) Compares all files inside the source document set against the target document set.

    Source (SP2016/SP2019) uses CSOM with the currently logged-in Windows user.
    Target (SharePoint Online) uses PnP PowerShell with the provided credentials.

    Output: a timestamped CSV with one result row per document set, and a JSON summary.

.PARAMETER CsvInputPath
    Path to CSV file with required columns:
    - SourceSiteUrl   : SP2016/SP2019 site URL (e.g. http://sp2019.contoso.local/sites/Finance)
    - TargetSiteUrl   : SharePoint Online site URL (e.g. https://tenant.sharepoint.com/sites/Finance)
    - LibraryName     : Document library name — must be identical in source and target
    - NewLocation     : Value to match against the "New Location" column on source document sets

.PARAMETER ClientId
    Azure AD app client ID used for PnP SharePoint Online authentication.

.PARAMETER TargetUsername
    SharePoint Online username used for authentication.

.PARAMETER TargetPassword
    SharePoint Online password used for authentication.

.PARAMETER NewLocationFieldName
    Internal (static) name of the "New Location" column in the source document library.
    Default: New_x0020_Location
    Tip: to find the internal name, open List Settings > click the column > read the
    'Field=' query parameter in the URL.

.PARAMETER OutputFolder
    Folder path for output files.
    Default: ./DocSetValidationLog-{timestamp}

.EXAMPLE
    .\Validate-DocumentSetMigration.ps1 `
        -CsvInputPath ".\SitePairs.csv" `
        -ClientId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -TargetUsername "admin@contoso.onmicrosoft.com" `
        -TargetPassword "Password123!" `
        -NewLocationFieldName "New_x0020_Location" `
        -OutputFolder "./DocSetValidationLog"
#>

[Diagnostics.CodeAnalysis.SuppressMessageAttribute(
    'PSAvoidUsingPlainTextForPassword', 'TargetPassword',
    Justification = 'Plain-text password parameter is intentional; matches the established project credential pattern.')]
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$CsvInputPath,

    [Parameter(Mandatory = $true)]
    [string]$ClientId,

    [Parameter(Mandatory = $true)]
    [string]$TargetUsername,

    [Parameter(Mandatory = $true)]
    [string]$TargetPassword,

    [string]$NewLocationFieldName = "New_x0020_Location",

    [int]$MaxRecordsPerFile = 10000,

    [string]$OutputFolder = "./DocSetValidationLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:Results            = @()
$script:ProcessedDocSets   = 0
$script:MatchedDocSets     = 0
$script:FailedDocSets      = 0
$script:FoundInTarget      = 0
$script:NotFoundInTarget   = 0
$script:FileDiffResults    = @()
$script:FileDiffFileCounter = 1
$script:FileDiffFilesCreated = 0
$script:FileDiffCurrentRowNumber = 0
$script:FileDiffCurrentNewLocation = ""
$script:FileDiffCurrentSafeNewLocation = ""
$script:CurrentFileDiffOutputFile = ""
$script:CurrentFileDiffRowCount = 0
$script:EmptyLocationResults    = @()
$script:EmptyLocationFileCounter = 1
$script:LastTargetFolderModified = $null
$script:Credential         = $null
$script:SourceContexts     = @{}
$script:LastTargetSiteUrl  = ""
$script:OutputFileCounter  = 1
$script:ResultFilesCreated = 0
$script:CurrentResultsFile = ""
$script:CurrentResultsFileRowCount = 0

if (-not (Test-Path $CsvInputPath)) {
    throw "CSV input file not found: $CsvInputPath"
}

if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
    Write-Host "Created output folder: $OutputFolder"
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
            $ctx.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
            $script:SourceContexts[$siteKey] = $ctx
        }
        catch {
            throw "Failed to create source context for '$SiteUrl': $_"
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

    $securePassword = ConvertTo-SecureString $TargetPassword -AsPlainText -Force
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
        Write-Verbose "Connecting to $SiteUrl"
        Connect-PnPOnline -Url $SiteUrl -Credentials $script:Credential -ClientId $ClientId -ErrorAction Stop
        $script:LastTargetSiteUrl = $SiteUrl
        Write-Host "Connected: $SiteUrl"
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
    $requiredColumns = @("SourceSiteUrl", "TargetSiteUrl", "LibraryName", "NewLocation")

    foreach ($column in $requiredColumns) {
        if (-not ($first.PSObject.Properties.Name -contains $column)) {
            throw "CSV must contain required column: $column"
        }
    }
}

# ==========================================
# Function: Update Validation Progress
# ==========================================
function Update-ValidationProgress {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$CurrentRowNumber,

        [Parameter(Mandatory = $true)]
        [int]$TotalRows,

        [string]$LibraryName = "",

        [string]$NewLocation = "",

        [string]$Phase = "Processing",

        [int]$CurrentDocumentSetNumber = 0,

        [int]$TotalDocumentSets = 0
    )

    $rowPercent = 0
    if ($TotalRows -gt 0) {
        $rowPercent = [Math]::Min(100, [int](($CurrentRowNumber / $TotalRows) * 100))
    }

    $rowStatusParts = @("Row $CurrentRowNumber of $TotalRows")
    if (-not [string]::IsNullOrWhiteSpace($LibraryName)) {
        $rowStatusParts += "Library='$LibraryName'"
    }
    if (-not [string]::IsNullOrWhiteSpace($NewLocation)) {
        $rowStatusParts += "NewLocation='$NewLocation'"
    }
    if (-not [string]::IsNullOrWhiteSpace($Phase)) {
        $rowStatusParts += "Phase=$Phase"
    }

    Write-Progress -Id 1 -Activity "Processing input rows" -Status ([string]::Join(' | ', $rowStatusParts)) -PercentComplete $rowPercent

    if ($TotalDocumentSets -gt 0) {
        $docPercent = [Math]::Min(100, [int](($CurrentDocumentSetNumber / $TotalDocumentSets) * 100))
        $docStatus = "Document set $CurrentDocumentSetNumber of $TotalDocumentSets | NewLocation='$NewLocation'"
        Write-Progress -Id 2 -ParentId 1 -Activity "Processing document sets" -Status $docStatus -CurrentOperation $Phase -PercentComplete $docPercent
    }
    else {
        Write-Progress -Id 2 -ParentId 1 -Activity "Processing document sets" -Completed
    }
}

# ==========================================
# Function: Clear Validation Progress
# ==========================================
function Clear-ValidationProgress {
    [CmdletBinding()]
    param()

    Write-Progress -Id 2 -Activity "Processing document sets" -Completed
    Write-Progress -Id 1 -Activity "Processing input rows" -Completed
}

# ==========================================
# Function: Get Source Document Sets by New Location
# ==========================================
function Get-SourceDocumentSets {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context,

        [Parameter(Mandatory = $true)]
        [string]$LibraryName,

        [Parameter(Mandatory = $true)]
        [string]$NewLocationValue
    )

    $list  = $null
    $items = $null

    try {
        $list = $Context.Web.Lists.GetByTitle($LibraryName)
        $Context.Load($list)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to get source library '$LibraryName': $_"
    }

    try {
        # Escape XML special characters in the filter value to build a safe CAML query
        $escapedValue = [System.Security.SecurityElement]::Escape($NewLocationValue)

        $camlQuery = New-Object Microsoft.SharePoint.Client.CamlQuery
        $camlQuery.ViewXml = "<View Scope='RecursiveAll'><Query><Where><And>" +
            "<Eq><FieldRef Name='FSObjType'/><Value Type='Integer'>1</Value></Eq>" +
            "<Eq><FieldRef Name='$NewLocationFieldName'/><Value Type='Text'>$escapedValue</Value></Eq>" +
            "</And></Where></Query></View>"

        $items = $list.GetItems($camlQuery)
        $Context.Load($items)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to query document sets in source library '$LibraryName' for NewLocation='$NewLocationValue': $_"
    }

    $docSets = @()
    foreach ($item in $items) {
        $docSets += [PSCustomObject]@{
            Name              = [string]$item["FileLeafRef"]
            ServerRelativeUrl = [string]$item["FileRef"]
            LastModified      = $item["Modified"]
        }
    }

    return $docSets
}

# ==========================================
# Function: Get Source Document Sets with NULL NewLocation
# ==========================================
function Get-NullLocationDocumentSets {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context,

        [Parameter(Mandatory = $true)]
        [string]$LibraryName,

        [Parameter(Mandatory = $true)]
        [string]$NewLocationFieldName
    )

    $list  = $null
    $items = $null

    try {
        $list = $Context.Web.Lists.GetByTitle($LibraryName)
        $Context.Load($list)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to get source library '$LibraryName': $_"
    }

    try {
        # Query for document sets where NewLocation is NULL (empty or not set)
        $camlQuery = New-Object Microsoft.SharePoint.Client.CamlQuery
        $camlQuery.ViewXml = "<View Scope='RecursiveAll'><Query><Where><And>" +
            "<Eq><FieldRef Name='FSObjType'/><Value Type='Integer'>1</Value></Eq>" +
            "<IsNull><FieldRef Name='$NewLocationFieldName'/></IsNull>" +
            "</And></Where></Query></View>"

        $items = $list.GetItems($camlQuery)
        $Context.Load($items)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to query document sets with NULL NewLocation in source library '$LibraryName': $_"
    }

    $docSets = @()
    foreach ($item in $items) {
        $docSets += [PSCustomObject]@{
            Name              = [string]$item["FileLeafRef"]
            ServerRelativeUrl = [string]$item["FileRef"]
            LastModified      = $item["Modified"]
        }
    }

    return $docSets
}

# ==========================================
# Function: Get Files in Source Document Set
# ==========================================
function Get-SourceDocumentSetFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context,

        [Parameter(Mandatory = $true)]
        [string]$DocumentSetServerRelativeUrl
    )

    $folder = $null
    $files  = $null

    try {
        $folder = $Context.Web.GetFolderByServerRelativeUrl($DocumentSetServerRelativeUrl)
        $files  = $folder.Files
        $Context.Load($folder)
        $Context.Load($files)
        $Context.ExecuteQuery()
    }
    catch {
        throw "Failed to get files from source document set '$DocumentSetServerRelativeUrl': $_"
    }

    $result = @()
    foreach ($file in $files) {
        $result += [PSCustomObject]@{
            Name              = $file.Name
            ServerRelativeUrl = $file.ServerRelativeUrl
            LastModified      = $file.TimeLastModified
        }
    }

    return $result
}

# ==========================================
# Function: Get Files in Target Document Set
# ==========================================
function Get-TargetDocumentSetFiles {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$TargetSiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$LibraryName,

        [Parameter(Mandatory = $true)]
        [string]$DocumentSetName
    )

    # Build server-relative path for the document set folder
    $siteUri    = New-Object System.Uri($TargetSiteUrl.TrimEnd('/'))
    $sitePath   = $siteUri.AbsolutePath.TrimEnd('/')
    $folderPath = "$sitePath/$LibraryName/$DocumentSetName"

    # Check that the document set exists as a folder in the target.
    # Returns a wrapper PSCustomObject { Found; Files } instead of a raw array so that
    # PS5.1's array-unwrapping cannot turn an empty-but-existing document set into $null
    # and cause it to be misreported as "NotFound".
    $folder = $null
    try {
        $folder = Get-PnPFolder -Url $folderPath -Includes TimeLastModified -ErrorAction Stop
    }
    catch {
        # Folder not found or inaccessible
        return [PSCustomObject]@{ Found = $false; Files = @() }
    }

    if ($null -eq $folder) {
        return [PSCustomObject]@{ Found = $false; Files = @() }
    }

    # Store target document-set last-modified for reporting
    $script:LastTargetFolderModified = $folder.TimeLastModified

    # Use Get-PnPFolderItem to enumerate files directly via the folder/files REST API.
    # Unlike a CAML list query, this bypasses the 5,000-item list view threshold because
    # it reads the folder's Files collection rather than issuing a list-level query.
    $folderItems = $null
    try {
        $folderSiteRelUrl = "$LibraryName/$DocumentSetName"
        $folderItems = @(Get-PnPFolderItem -FolderSiteRelativeUrl $folderSiteRelUrl -ItemType File -ErrorAction Stop)
    }
    catch {
        throw "Failed to get files from target document set '$folderPath': $_"
    }

    $result = @()
    foreach ($item in $folderItems) {
        $result += [PSCustomObject]@{
            Name              = $item.Name
            ServerRelativeUrl = $item.ServerRelativeUrl
            LastModified      = $item.TimeLastModified
        }
    }

    return [PSCustomObject]@{ Found = $true; Files = $result }
}

# ==========================================
# Function: Compare Document Set Files
# ==========================================
function Compare-DocumentSetFiles {
    [CmdletBinding()]
    param(
        [AllowEmptyCollection()]
        [Parameter(Mandatory = $true)]
        [array]$SourceFiles,

        [AllowEmptyCollection()]
        [Parameter(Mandatory = $true)]
        [array]$TargetFiles
    )

    # Treat null as empty for defensive safety in PS 5.1 binding/output edge cases.
    if ($null -eq $SourceFiles) {
        $SourceFiles = @()
    }
    if ($null -eq $TargetFiles) {
        $TargetFiles = @()
    }

    $targetLookup = @{}
    foreach ($f in $TargetFiles) {
        $targetLookup[$f.Name.ToLowerInvariant()] = $f.Name
    }

    $sourceLookup = @{}
    foreach ($f in $SourceFiles) {
        $sourceLookup[$f.Name.ToLowerInvariant()] = $f.Name
    }

    $missingInTarget = @()
    foreach ($f in $SourceFiles) {
        if (-not $targetLookup.ContainsKey($f.Name.ToLowerInvariant())) {
            $missingInTarget += $f
        }
    }

    $extraInTarget = @()
    foreach ($f in $TargetFiles) {
        if (-not $sourceLookup.ContainsKey($f.Name.ToLowerInvariant())) {
            $extraInTarget += $f
        }
    }

    return [PSCustomObject]@{
        MissingInTarget = [array]$missingInTarget
        ExtraInTarget   = [array]$extraInTarget
    }
}

# ==========================================
# Function: Add Result Row (helper)
# ==========================================
function New-ResultRow {
    [CmdletBinding()]
    param(
        [int]    $RowNumber,
        [string] $SourceSiteUrl,
        [string] $TargetSiteUrl,
        [string] $LibraryName,
        [string] $NewLocation,
        [string] $DocumentSetName,
        [string] $Status,
        [int]    $SourceFileCount,
        [int]    $TargetFileCount,
        [bool]   $IsSuccessful,
        [string] $Message,
        [string] $SourceDocSetLastModified = "",
        [string] $TargetDocSetLastModified = ""
    )

    return [PSCustomObject]@{
        RowNumber                = $RowNumber
        SourceSiteUrl            = $SourceSiteUrl
        TargetSiteUrl            = $TargetSiteUrl
        LibraryName              = $LibraryName
        NewLocation              = $NewLocation
        DocumentSetName          = $DocumentSetName
        Status                   = $Status
        SourceFileCount          = $SourceFileCount
        TargetFileCount          = $TargetFileCount
        IsSuccessful             = $IsSuccessful
        Message                  = $Message
        SourceDocSetLastModified = $SourceDocSetLastModified
        TargetDocSetLastModified = $TargetDocSetLastModified
        Timestamp                = (Get-Date).ToString("s")
    }
}

# ==========================================
# Function: Start New Results Output File
# ==========================================
function Start-NewResultsOutputFile {
    [CmdletBinding()]
    param()

    $timestamp  = Get-Date -Format "yyyyMMdd-HHmmss"
    $script:CurrentResultsFile = Join-Path $OutputFolder "DocSetValidationResults_Part$($script:OutputFileCounter)_$timestamp.csv"
    $script:CurrentResultsFileRowCount = 0
    $script:OutputFileCounter++
    $script:ResultFilesCreated++
    Write-Host "Started results output file: $($script:CurrentResultsFile)" -ForegroundColor DarkCyan
}

# ==========================================
# Function: Append Result Row to CSV File
# ==========================================
function Add-ResultRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$ResultRow
    )

    if ([string]::IsNullOrWhiteSpace($script:CurrentResultsFile) -or
        $script:CurrentResultsFileRowCount -ge $MaxRecordsPerFile) {
        Start-NewResultsOutputFile
    }

    try {
        if ($script:CurrentResultsFileRowCount -eq 0) {
            $ResultRow | Export-Csv -Path $script:CurrentResultsFile -NoTypeInformation -Encoding UTF8 -Force
        }
        else {
            $ResultRow | Export-Csv -Path $script:CurrentResultsFile -NoTypeInformation -Encoding UTF8 -Append
        }

        $script:CurrentResultsFileRowCount++

        if ($script:CurrentResultsFileRowCount -ge $MaxRecordsPerFile) {
            Write-Host "Exported $($script:CurrentResultsFileRowCount) result(s) to: $($script:CurrentResultsFile)" -ForegroundColor Green
            $script:CurrentResultsFile = ""
            $script:CurrentResultsFileRowCount = 0
        }
    }
    catch {
        Write-Error "Failed to append result row to file: $_"
    }
}

# ==========================================
# Function: Convert Value to Safe File Name Segment
# ==========================================
function ConvertTo-SafeFileNameSegment {
    [CmdletBinding()]
    param(
        [AllowEmptyString()]
        [string]$Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "BlankNewLocation"
    }

    $safeValue = $Value.Trim()
    foreach ($invalidChar in [System.IO.Path]::GetInvalidFileNameChars()) {
        $safeValue = $safeValue.Replace([string]$invalidChar, "-")
    }

    $safeValue = $safeValue -replace '\s+', '_'
    $safeValue = $safeValue.Trim(' ', '.', '_', '-')

    if ([string]::IsNullOrWhiteSpace($safeValue)) {
        return "BlankNewLocation"
    }

    if ($safeValue.Length -gt 80) {
        $safeValue = $safeValue.Substring(0, 80)
    }

    return $safeValue
}

# ==========================================
# Function: Start New File Diff Output File
# ==========================================
function Start-NewFileDiffOutputFile {
    [CmdletBinding()]
    param()

    $timestamp  = Get-Date -Format "yyyyMMdd-HHmmss"
    $script:CurrentFileDiffOutputFile = Join-Path $OutputFolder "FileDiff_Part$($script:FileDiffFileCounter)_$timestamp_Row$($script:FileDiffCurrentRowNumber)_$($script:FileDiffCurrentSafeNewLocation).csv"
    $script:CurrentFileDiffRowCount = 0
    $script:FileDiffFileCounter++
    $script:FileDiffFilesCreated++
    Write-Host "Started file diff output file: $($script:CurrentFileDiffOutputFile)" -ForegroundColor DarkCyan
}

# ==========================================
# Function: Append File Diff Rows to CSV File
# ==========================================
function Append-FileDiffRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$DiffRows
    )

    if ($null -eq $DiffRows -or $DiffRows.Count -eq 0) {
        return
    }

    $startIndex = 0
    while ($startIndex -lt $DiffRows.Count) {
        if ([string]::IsNullOrWhiteSpace($script:CurrentFileDiffOutputFile) -or
            $script:CurrentFileDiffRowCount -ge $MaxRecordsPerFile) {
            Start-NewFileDiffOutputFile
        }

        $remainingCapacity = $MaxRecordsPerFile - $script:CurrentFileDiffRowCount
        $rowsToWriteCount = [Math]::Min($remainingCapacity, ($DiffRows.Count - $startIndex))
        $endIndex = $startIndex + $rowsToWriteCount - 1
        $rowsToWrite = @($DiffRows[$startIndex..$endIndex])

        try {
            if ($script:CurrentFileDiffRowCount -eq 0) {
                $rowsToWrite | Export-Csv -Path $script:CurrentFileDiffOutputFile -NoTypeInformation -Encoding UTF8 -Force
            }
            else {
                $rowsToWrite | Export-Csv -Path $script:CurrentFileDiffOutputFile -NoTypeInformation -Encoding UTF8 -Append
            }

            $script:CurrentFileDiffRowCount += $rowsToWriteCount

            if ($script:CurrentFileDiffRowCount -ge $MaxRecordsPerFile) {
                Write-Host "Exported $($script:CurrentFileDiffRowCount) file diff row(s) to: $($script:CurrentFileDiffOutputFile)" -ForegroundColor Green
                $script:CurrentFileDiffOutputFile = ""
                $script:CurrentFileDiffRowCount = 0
            }
        }
        catch {
            Write-Error "Failed to append file diff rows to file: $_"
            return
        }

        $startIndex = $endIndex + 1
    }
}

# ==========================================
# Function: Add File Diff Rows
# ==========================================
function Add-FileDiffResults {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$RowNumber,

        [Parameter(Mandatory = $true)]
        [string]$NewLocation,

        [Parameter(Mandatory = $true)]
        [array]$DiffRows
    )

    if ($null -eq $DiffRows -or $DiffRows.Count -eq 0) {
        return
    }

    if ($script:FileDiffCurrentRowNumber -ne $RowNumber) {
        if ($script:CurrentFileDiffRowCount -gt 0 -and -not [string]::IsNullOrWhiteSpace($script:CurrentFileDiffOutputFile)) {
            Write-Host "Exported $($script:CurrentFileDiffRowCount) file diff row(s) to: $($script:CurrentFileDiffOutputFile)" -ForegroundColor Green
        }

        $script:FileDiffCurrentRowNumber = $RowNumber
        $script:FileDiffCurrentNewLocation = $NewLocation
        $script:FileDiffCurrentSafeNewLocation = ConvertTo-SafeFileNameSegment -Value $NewLocation
        $script:FileDiffFileCounter = 1
        $script:CurrentFileDiffOutputFile = ""
        $script:CurrentFileDiffRowCount = 0
    }

    Append-FileDiffRows -DiffRows $DiffRows
}

# ==========================================
# Function: Finalize File Diff Rows for Current Input Row
# ==========================================
function Complete-FileDiffRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$RowNumber
    )

    if ($script:FileDiffCurrentRowNumber -ne $RowNumber) {
        return
    }

    if ($script:CurrentFileDiffRowCount -gt 0 -and -not [string]::IsNullOrWhiteSpace($script:CurrentFileDiffOutputFile)) {
        Write-Host "Exported $($script:CurrentFileDiffRowCount) file diff row(s) to: $($script:CurrentFileDiffOutputFile)" -ForegroundColor Green
    }

    $script:FileDiffCurrentRowNumber = 0
    $script:FileDiffCurrentNewLocation = ""
    $script:FileDiffCurrentSafeNewLocation = ""
    $script:FileDiffFileCounter = 1
    $script:CurrentFileDiffOutputFile = ""
    $script:CurrentFileDiffRowCount = 0
}

# ==========================================
# Function: Export Empty Location Results Batch to CSV File
# ==========================================
function Export-EmptyLocationResults {
    [CmdletBinding()]
    param()

    if ($script:EmptyLocationResults.Count -eq 0) {
        return
    }

    $timestamp  = Get-Date -Format "yyyyMMdd-HHmmss"
    $outputFile = Join-Path $OutputFolder "EmptyLocationDocumentSets_Part$($script:EmptyLocationFileCounter)_$timestamp.csv"

    try {
        $script:EmptyLocationResults | Export-Csv -Path $outputFile -NoTypeInformation -Encoding UTF8 -Force
        Write-Host "Exported $($script:EmptyLocationResults.Count) empty location document set(s) to: $outputFile" -ForegroundColor Green
        $script:EmptyLocationFileCounter++
        $script:EmptyLocationResults = @()
    }
    catch {
        Write-Error "Failed to export empty location results to file: $_"
    }
}

# ==========================================
# Function: Main Validation Processing
# ==========================================
function Invoke-DocSetValidation {
    [CmdletBinding()]
    param()

    $startTime = Get-Date
    $rows      = Import-Csv -Path $CsvInputPath
    $totalRows = $rows.Count
    Test-CsvFormat -Rows $rows

    Write-Host "Loaded $($rows.Count) row(s) from CSV." -ForegroundColor Green

    $rowIndex = 0
    foreach ($row in $rows) {
        $rowIndex++

        $sourceSiteUrl = [string]$row.SourceSiteUrl
        $targetSiteUrl = [string]$row.TargetSiteUrl
        $libraryName   = [string]$row.LibraryName
        $newLocation   = [string]$row.NewLocation

        Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
            -LibraryName $libraryName -NewLocation $newLocation -Phase "Validating row input"

        # --- Blank field guard ---
        if ([string]::IsNullOrWhiteSpace($sourceSiteUrl) -or
            [string]::IsNullOrWhiteSpace($targetSiteUrl) -or
            [string]::IsNullOrWhiteSpace($libraryName)   -or
            [string]::IsNullOrWhiteSpace($newLocation)) {

            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName "" -Status "Failed" `
                -SourceFileCount 0 -TargetFileCount 0 `
                -IsSuccessful $false `
                -Message "SourceSiteUrl, TargetSiteUrl, LibraryName, and NewLocation are all required")

            $script:ProcessedDocSets++
            $script:FailedDocSets++
            continue
        }

        $sourceSiteUrl = $sourceSiteUrl.Trim()
        $targetSiteUrl = $targetSiteUrl.Trim()
        $libraryName   = $libraryName.Trim()
        $newLocation   = $newLocation.Trim()

        Write-Host "Row $rowIndex/$($rows.Count): Library='$libraryName'  NewLocation='$newLocation'"
        Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
            -LibraryName $libraryName -NewLocation $newLocation -Phase "Connecting to source and target"

        # --- Source CSOM context ---
        $sourceContext = $null
        try {
            $sourceContext = Get-SourceContext -SiteUrl $sourceSiteUrl
        }
        catch {
            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName "" -Status "Failed" `
                -SourceFileCount 0 -TargetFileCount 0 `
                -IsSuccessful $false `
                -Message "Could not connect to source site: $_")

            $script:ProcessedDocSets++
            $script:FailedDocSets++
            continue
        }

        # --- SPO PnP connection ---
        try {
            Get-PnPConnectionForSite -SiteUrl $targetSiteUrl
        }
        catch {
            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName "" -Status "Failed" `
                -SourceFileCount 0 -TargetFileCount 0 `
                -IsSuccessful $false `
                -Message "Could not connect to target site: $_")

            $script:ProcessedDocSets++
            $script:FailedDocSets++
            continue
        }

        # --- Special handler for CheckEmptyDocumentSet mode ---
        if ($newLocation -eq "CheckEmptyDocumentSet") {
            Write-Host "  [Special Mode: CheckEmptyDocumentSet] Querying for document sets with NULL NewLocation..."
            Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                -LibraryName $libraryName -NewLocation $newLocation -Phase "Querying NULL NewLocation document sets"

            $docSets = @()
            try {
                $docSets = @(Get-NullLocationDocumentSets -Context $sourceContext `
                    -LibraryName $libraryName -NewLocationFieldName $NewLocationFieldName)
            }
            catch {
                Write-Error "Failed to query document sets with NULL NewLocation: $_" -ForegroundColor Red
                continue
            }

            if ($docSets.Count -eq 0) {
                Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                    -LibraryName $libraryName -NewLocation $newLocation -Phase "No NULL NewLocation document sets found"
                Write-Host "  No document sets found with NULL NewLocation." -ForegroundColor Yellow
            }
            else {
                Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                    -LibraryName $libraryName -NewLocation $newLocation -Phase "Exporting NULL NewLocation document sets" `
                    -CurrentDocumentSetNumber 0 -TotalDocumentSets $docSets.Count
                Write-Host "  Found $($docSets.Count) document set(s) with NULL NewLocation — exporting..." -ForegroundColor Cyan

                $nullDocSetIndex = 0
                foreach ($docSet in $docSets) {
                    $nullDocSetIndex++
                    Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                        -LibraryName $libraryName -NewLocation $newLocation -Phase "Exporting NULL NewLocation document sets" `
                        -CurrentDocumentSetNumber $nullDocSetIndex -TotalDocumentSets $docSets.Count

                    $script:EmptyLocationResults += [PSCustomObject]@{
                        SourceSiteUrl   = $sourceSiteUrl
                        LibraryName     = $libraryName
                        DocumentSetName = $docSet.Name
                        ServerRelativeUrl = $docSet.ServerRelativeUrl
                        LastModified    = ([string]$docSet.LastModified)
                        Timestamp       = (Get-Date).ToString("s")
                    }

                    if ($script:EmptyLocationResults.Count -ge $MaxRecordsPerFile) {
                        Export-EmptyLocationResults
                    }
                }
            }

            Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                -LibraryName $libraryName -NewLocation $newLocation -Phase "Completed special row"
            continue
        }

        # --- Query source document sets by NewLocation ---
        $docSets = @()
        Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
            -LibraryName $libraryName -NewLocation $newLocation -Phase "Querying document sets"
        try {
            $docSets = @(Get-SourceDocumentSets -Context $sourceContext `
                -LibraryName $libraryName -NewLocationValue $newLocation)
        }
        catch {
            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName "" -Status "Failed" `
                -SourceFileCount 0 -TargetFileCount 0 `
                -IsSuccessful $false `
                -Message "Failed to query source document sets: $_")

            $script:ProcessedDocSets++
            $script:FailedDocSets++
            continue
        }

        if ($docSets.Count -eq 0) {
            Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                -LibraryName $libraryName -NewLocation $newLocation -Phase "No document sets found"
            Write-Host "  No document sets found with NewLocation='$newLocation' — skipping row." -ForegroundColor Yellow

            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName "" -Status "Skipped" `
                -SourceFileCount 0 -TargetFileCount 0 `
                -IsSuccessful $true `
                -Message "No document sets found in source with NewLocation='$newLocation'")

            $script:ProcessedDocSets++
            $script:MatchedDocSets++
            continue
        }

        Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
            -LibraryName $libraryName -NewLocation $newLocation -Phase "Query completed" `
            -CurrentDocumentSetNumber 0 -TotalDocumentSets $docSets.Count
        Write-Host "  Found $($docSets.Count) document set(s) — validating each..." -ForegroundColor Cyan

        # Build host prefix for constructing full file URLs
        $sourceUri2  = New-Object System.Uri($sourceSiteUrl)
        $sourceHost  = "$($sourceUri2.Scheme)://$($sourceUri2.Authority)"
        $targetUri2  = New-Object System.Uri($targetSiteUrl.TrimEnd('/'))
        $targetHost  = "$($targetUri2.Scheme)://$($targetUri2.Authority)"

        # --- Per-document-set validation ---
        $docSetIndex = 0
        foreach ($docSet in $docSets) {
            $docSetIndex++
            Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
                -LibraryName $libraryName -NewLocation $newLocation -Phase "Validating document sets" `
                -CurrentDocumentSetNumber $docSetIndex -TotalDocumentSets $docSets.Count

            Write-Host "    Document set: $($docSet.Name)"

            # Get source files
            $sourceFiles = @()
            try {
                $sourceFiles = @(Get-SourceDocumentSetFiles -Context $sourceContext `
                    -DocumentSetServerRelativeUrl $docSet.ServerRelativeUrl)
            }
            catch {
                Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                    -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                    -LibraryName $libraryName -NewLocation $newLocation `
                    -DocumentSetName $docSet.Name -Status "Failed" `
                    -SourceFileCount 0 -TargetFileCount 0 `
                    -IsSuccessful $false `
                    -Message "Failed to read source document set files: $_")

                $script:ProcessedDocSets++
                $script:FailedDocSets++
                continue
            }

            # Get target files — .Found = $false means document set was not found in target
            $script:LastTargetFolderModified = $null
            $targetResult = $null
            try {
                $targetResult = Get-TargetDocumentSetFiles -TargetSiteUrl $targetSiteUrl `
                    -LibraryName $libraryName -DocumentSetName $docSet.Name
            }
            catch {
                Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                    -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                    -LibraryName $libraryName -NewLocation $newLocation `
                    -DocumentSetName $docSet.Name -Status "Failed" `
                    -SourceFileCount $sourceFiles.Count -TargetFileCount 0 `
                    -IsSuccessful $false `
                    -Message "Failed to read target document set files: $_" `
                    -SourceDocSetLastModified ([string]$docSet.LastModified))

                $script:ProcessedDocSets++
                $script:FailedDocSets++
                continue
            }

            if (-not $targetResult.Found) {
                # Document set entirely missing from target
                Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                    -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                    -LibraryName $libraryName -NewLocation $newLocation `
                    -DocumentSetName $docSet.Name -Status "NotFound" `
                    -SourceFileCount $sourceFiles.Count -TargetFileCount 0 `
                    -IsSuccessful $false `
                    -Message "Document set '$($docSet.Name)' does not exist in target library '$libraryName'" `
                    -SourceDocSetLastModified ([string]$docSet.LastModified))

                # Log each source file as missing in target in the diff file
                $docSetDiffRows = @()
                foreach ($srcFile in $sourceFiles) {
                    $docSetDiffRows += [PSCustomObject]@{
                        SourceSiteUrl   = $sourceSiteUrl
                        TargetSiteUrl   = $targetSiteUrl
                        LibraryName     = $libraryName
                        NewLocation     = $newLocation
                        DocumentSetName = $docSet.Name
                        FileName        = $srcFile.Name
                        SourceFileUrl   = "$sourceHost$($srcFile.ServerRelativeUrl)"
                        TargetFileUrl   = ""
                        Status          = "Missing in Target"
                        LastModified    = $srcFile.LastModified
                    }
                }

                Add-FileDiffResults -RowNumber $rowIndex -NewLocation $newLocation -DiffRows $docSetDiffRows

                Write-Host "      NOT FOUND in target." -ForegroundColor Red
                $script:ProcessedDocSets++
                $script:FailedDocSets++
                $script:NotFoundInTarget++
                continue
            }

            # Document set found in target
            $script:FoundInTarget++
            # $targetResult.Files is always an array (empty or populated); wrap in @() for PS5.1 safety
            $targetFiles = @($targetResult.Files)

            # Document set found — compare file lists
            $comparison   = Compare-DocumentSetFiles -SourceFiles $sourceFiles -TargetFiles $targetFiles

            $status       = ""
            $message      = ""
            $isSuccessful = $false

            if ($comparison.MissingInTarget.Count -eq 0 -and $comparison.ExtraInTarget.Count -eq 0) {
                $status       = "Matched"
                $isSuccessful = $true
                $message      = "All $($sourceFiles.Count) file(s) match"
                $script:MatchedDocSets++
                Write-Host "      Matched ($($sourceFiles.Count) file(s))." -ForegroundColor Green
            }
            else {
                $status       = "HasDifferences"
                $isSuccessful = $false

                $parts = @()
                if ($comparison.MissingInTarget.Count -gt 0) {
                    $parts += "Missing in target: $($comparison.MissingInTarget.Count) file(s)"
                }
                if ($comparison.ExtraInTarget.Count -gt 0) {
                    $parts += "Extra in target: $($comparison.ExtraInTarget.Count) file(s)"
                }
                $message = [string]::Join("; ", $parts)
                $script:FailedDocSets++
                Write-Host "      DIFFERENCES: $message" -ForegroundColor Yellow

                $docSetDiffRows = @()

                # Log missing files (in source, absent from target) to the diff file
                foreach ($f in $comparison.MissingInTarget) {
                    $docSetDiffRows += [PSCustomObject]@{
                        SourceSiteUrl   = $sourceSiteUrl
                        TargetSiteUrl   = $targetSiteUrl
                        LibraryName     = $libraryName
                        NewLocation     = $newLocation
                        DocumentSetName = $docSet.Name
                        FileName        = $f.Name
                        SourceFileUrl   = "$sourceHost$($f.ServerRelativeUrl)"
                        TargetFileUrl   = ""
                        Status          = "Missing in Target"
                        LastModified    = $f.LastModified
                    }
                }

                # Log extra files (in target, absent from source) to the diff file
                foreach ($f in $comparison.ExtraInTarget) {
                    $docSetDiffRows += [PSCustomObject]@{
                        SourceSiteUrl   = $sourceSiteUrl
                        TargetSiteUrl   = $targetSiteUrl
                        LibraryName     = $libraryName
                        NewLocation     = $newLocation
                        DocumentSetName = $docSet.Name
                        FileName        = $f.Name
                        SourceFileUrl   = ""
                        TargetFileUrl   = "$targetHost$($f.ServerRelativeUrl)"
                        Status          = "Missing in Source"
                        LastModified    = $f.LastModified
                    }
                }

                Add-FileDiffResults -RowNumber $rowIndex -NewLocation $newLocation -DiffRows $docSetDiffRows
            }

            Add-ResultRow -ResultRow (New-ResultRow -RowNumber $rowIndex `
                -SourceSiteUrl $sourceSiteUrl -TargetSiteUrl $targetSiteUrl `
                -LibraryName $libraryName -NewLocation $newLocation `
                -DocumentSetName $docSet.Name -Status $status `
                -SourceFileCount $sourceFiles.Count -TargetFileCount $targetFiles.Count `
                -IsSuccessful $isSuccessful `
                -Message $message `
                -SourceDocSetLastModified ([string]$docSet.LastModified) `
                -TargetDocSetLastModified ([string]$script:LastTargetFolderModified))

            $script:ProcessedDocSets++
        }

        Update-ValidationProgress -CurrentRowNumber $rowIndex -TotalRows $totalRows `
            -LibraryName $libraryName -NewLocation $newLocation -Phase "Completed row" `
            -CurrentDocumentSetNumber $docSets.Count -TotalDocumentSets $docSets.Count
        Complete-FileDiffRow -RowNumber $rowIndex
    }

    Clear-ValidationProgress

    # Emit final status for partially-filled current results file
    if ($script:CurrentResultsFileRowCount -gt 0 -and -not [string]::IsNullOrWhiteSpace($script:CurrentResultsFile)) {
        Write-Host "Exported $($script:CurrentResultsFileRowCount) result(s) to: $($script:CurrentResultsFile)" -ForegroundColor Green
    }
    if ($script:CurrentFileDiffRowCount -gt 0 -and -not [string]::IsNullOrWhiteSpace($script:CurrentFileDiffOutputFile)) {
        Write-Host "Exported $($script:CurrentFileDiffRowCount) file diff row(s) to: $($script:CurrentFileDiffOutputFile)" -ForegroundColor Green
    }
    if ($script:EmptyLocationResults.Count -gt 0) {
        Export-EmptyLocationResults
    }

    # ==========================================
    # Output Summary
    # ==========================================
    $endTime  = Get-Date
    $duration = $endTime - $startTime

    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"

    $summary = @{
        ExecutionTime             = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalCsvRows              = $rows.Count
        TotalDocSetsValidated     = $script:ProcessedDocSets
        DocSetsFoundInTarget      = $script:FoundInTarget
        DocSetsNotFoundInTarget   = $script:NotFoundInTarget
        MatchedDocSets            = $script:MatchedDocSets
        FailedOrMismatchedDocSets = $script:FailedDocSets
        ResultFilesCreated        = $script:ResultFilesCreated
        FileDiffFilesCreated      = $script:FileDiffFilesCreated
        TimestampStart            = $startTime.ToString("s")
        TimestampEnd              = $endTime.ToString("s")
    }

    $summary | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Document Set Migration Validation Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total CSV Rows:           $($summary.TotalCsvRows)"
    Write-Host "Total Doc Sets Validated: $($summary.TotalDocSetsValidated)"
    Write-Host "Found in Target:          $($summary.DocSetsFoundInTarget)" -ForegroundColor Green
    Write-Host "Not Found in Target:      $($summary.DocSetsNotFoundInTarget)" -ForegroundColor $(if ($summary.DocSetsNotFoundInTarget -gt 0) { "Red" } else { "Green" })
    Write-Host "Matched (files match):    $($summary.MatchedDocSets)" -ForegroundColor Green
    Write-Host "Failed / Mismatched:      $($summary.FailedOrMismatchedDocSets)" -ForegroundColor $(if ($summary.FailedOrMismatchedDocSets -gt 0) { "Red" } else { "Green" })
    Write-Host "Result Files Created:     $($summary.ResultFilesCreated)"
    Write-Host "File Diff Files Created:  $($summary.FileDiffFilesCreated)"
    Write-Host "Summary JSON:             $summaryFile"
    Write-Host "Execution Time:           $($summary.ExecutionTime)"
    Write-Host "Output Location:          $OutputFolder"
    Write-Host "==========================================" -ForegroundColor Cyan
}

# ==========================================
# Main Execution
# ==========================================
try {
    Write-Host "Starting document set migration validation..." -ForegroundColor Cyan
    Import-CSOMAssemblies
    Initialize-Credential
    Invoke-DocSetValidation
}
catch {
    Write-Error "Fatal error: $_"
    exit 1
}
finally {
    Clear-ValidationProgress
    Disconnect-PnPOnline -ErrorAction SilentlyContinue
    Write-Host "Script execution completed." -ForegroundColor Cyan
}

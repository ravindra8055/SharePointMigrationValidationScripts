#Requires -Version 5.1
<#
.SYNOPSIS
    Compares folder contents between SharePoint 2019 (source) and SharePoint Online (target).
    
.DESCRIPTION
    Validates migration by checking if all items in source folders exist in target folders.
    Uses CSOM for SP2019 and PnP PowerShell for SPO with client credentials.
    
.PARAMETER CsvInputPath
    Path to CSV file with one of the following formats:
    1) FolderPath (for example: Shared Documents/Folder1/Folder2)
    2) SourceFolderUrl, TargetFolderUrl (legacy format)

.PARAMETER SourceSiteUrl
    SharePoint 2019 site URL used as base for FolderPath rows
    
.PARAMETER OutputFolder
    Folder path for output CSV files (max 10,000 records per file)
    Default: ./MigrationLog-{timestamp}
    
.PARAMETER TargetSiteUrl
    SharePoint Online target site URL used as base for FolderPath rows
    (e.g., https://tenant.sharepoint.com/sites/Finance)
    
.PARAMETER ClientId
    Azure AD app client ID for SPO authentication
    
.PARAMETER TargetUsername
    SharePoint Online username (hardcoded credentials)
    
.PARAMETER TargetPassword
    SharePoint Online password (hardcoded credentials)
    
.PARAMETER MaxRecordsPerFile
    Maximum records per output CSV (default: 10000)
    
.PARAMETER ThreadCount
    Number of parallel threads for processing (default: 4, optimized for site collections)

.EXAMPLE
    .\Compare-MigratedFolders.ps1 -CsvInputPath "folders.csv" -TargetSiteUrl "https://tenant.sharepoint.com/sites/Finance" `
        -ClientId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -TargetUsername "user@tenant.onmicrosoft.com" `
        -TargetPassword "Password123" -OutputFolder "./Logs"
#>

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

    [string]$OutputFolder = "./MigrationLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')",

    [int]$MaxRecordsPerFile = 10000,

    [int]$ThreadCount = 4
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:MissingItems = @()
$script:ProcessedFolders = 0
$script:FailedFolders = 0
$script:TotalDifferences = 0
$script:TotalMissingInTarget = 0
$script:TotalExtraInTarget = 0
$script:OutputFileCounter = 1
$script:SourceContexts = @{}
$script:TargetContext = $null
$script:LogLock = [System.Threading.Mutex]::new($false)
$script:FolderCountLog = @()
$script:ThresholdFolderCounts = @{}

# Create output folder if it doesn't exist
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
        $clientDll = Join-Path $path "Microsoft.SharePoint.Client.dll"

        if ((Test-Path $runtimeDll) -and (Test-Path $clientDll)) {
            Add-Type -Path $runtimeDll -ErrorAction SilentlyContinue
            Add-Type -Path $clientDll -ErrorAction SilentlyContinue
            $loaded = $true
            Write-Host "[OK] CSOM assemblies loaded from $path"
            break
        }
    }

    if (-not $loaded) {
        throw "CSOM assemblies not found. Ensure SharePoint Server with CSOM or client libraries are installed."
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
            $ctx.RequestTimeout = 120000  # 2 minutes
            
            # Test connection
            $web = $ctx.Web
            $ctx.Load($web)
            $ctx.ExecuteQuery()
            
            $script:SourceContexts[$siteKey] = $ctx
            Write-Verbose "Source context created: $SiteUrl"
        }
        catch {
            Write-Error "Failed to create source context for $SiteUrl : $_"
            return $null
        }
    }

    return $script:SourceContexts[$siteKey]
}

# ==========================================
# Function: Initialize Target Connection (PnP)
# ==========================================
function Initialize-TargetConnection {
    [CmdletBinding()]
    param()

    try {
        # Check if PnP.PowerShell is installed
        <#if (-not (Get-Module -Name PnP.PowerShell -ListAvailable)) {
            Write-Warning "PnP.PowerShell not found. Installing..."
            Install-Module -Name PnP.PowerShell -Scope CurrentUser -Force -AllowClobber
        }#>

        Write-Host "Connecting to SharePoint Online..."
        $securePassword = ConvertTo-SecureString $TargetPassword -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential($TargetUsername, $securePassword)

        Connect-PnPOnline -Url $TargetSiteUrl -Credentials $credential -ClientId $ClientId -ErrorAction Stop
        $script:TargetContext = Get-PnPConnection
        Write-Host "[OK] Connected to SharePoint Online: $TargetSiteUrl"
    }
    catch {
        throw "Failed to connect to SharePoint Online: $_"
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

    $trimmed = $FolderPath.Trim()
    $trimmed = $trimmed -replace '\\', '/'
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

    $base = $SiteUrl.TrimEnd('/')
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

    $normalizedUrl = $Url.Trim() -replace '\\', '/'
    $normalizedSiteUrl = $SiteUrl.TrimEnd('/')

    if ($normalizedUrl.StartsWith($normalizedSiteUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        $relativePart = $normalizedUrl.Substring($normalizedSiteUrl.Length).TrimStart('/')
        $siteUri = New-Object System.Uri($normalizedSiteUrl)
        $sitePath = $siteUri.AbsolutePath.TrimEnd('/')

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
    $siteUri = New-Object System.Uri($SiteUrl)
    $sitePath = $siteUri.AbsolutePath.TrimEnd('/')

    if ((-not [string]::IsNullOrWhiteSpace($sitePath)) -and ($sitePath -ne '/') -and $serverRelativePath.StartsWith($sitePath, [System.StringComparison]::OrdinalIgnoreCase)) {
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
            FolderPath = $normalized
        }
    }

    if (($Row.PSObject.Properties.Name -contains "SourceFolderUrl") -and ($Row.PSObject.Properties.Name -contains "TargetFolderUrl")) {
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
            FolderPath = $derivedFolderPath
        }
    }

    throw "CSV must include either FolderPath or SourceFolderUrl,TargetFolderUrl columns"
}

# ==========================================
# Function: Get Folder Items from Source (SP2019)
# ==========================================
function Get-SourceFolderItems {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderUrl,

        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context
    )

    try {
        $serverRelativePath = Get-ServerRelativePathFromUrl -Url $FolderUrl -SiteUrl $SourceSiteUrl

        $resourcePathType = "Microsoft.SharePoint.Client.ResourcePath" -as [type]
        $hasPathMethod = $false

        if ($resourcePathType) {
            $pathMethod = [Microsoft.SharePoint.Client.Web].GetMethod(
                "GetFolderByServerRelativePath",
                [System.Reflection.BindingFlags]::Public -bor [System.Reflection.BindingFlags]::Instance,
                $null,
                [type[]]@($resourcePathType),
                $null
            )
            $hasPathMethod = ($null -ne $pathMethod)
        }

        if ($hasPathMethod) {
            try {
                $resourcePath = [Microsoft.SharePoint.Client.ResourcePath]::FromDecodedUrl($serverRelativePath)
                $folder = $Context.Web.GetFolderByServerRelativePath($resourcePath)
            }
            catch [System.Management.Automation.MethodException] {
                $folder = $Context.Web.GetFolderByServerRelativeUrl($serverRelativePath)
            }
        }
        else {
            $folder = $Context.Web.GetFolderByServerRelativeUrl($serverRelativePath)
        }
        
        # PowerShell 5.1 can fail casting ScriptBlock to CSOM expression trees.
        # Use plain Load calls for compatibility.
        $items = $folder.Files
        $subFolders = $folder.Folders

        $Context.Load($items)
        $Context.Load($subFolders)

        $Context.ExecuteQuery()

        $result = @()
        
        # Add files
        foreach ($file in $items) {
            $fileLastModified = $null
            if ($file.IsPropertyAvailable("TimeLastModified")) {
                $fileLastModified = $file.TimeLastModified
            }

            $result += @{
                Name = $file.Name
                Type = "File"
                LastModified = $fileLastModified
            }
        }

        # Add folders
        foreach ($subFolderItem in $subFolders) {
            $folderLastModified = $null
            if ($subFolderItem.IsPropertyAvailable("TimeLastModified")) {
                $folderLastModified = $subFolderItem.TimeLastModified
            }

            $result += @{
                Name = $subFolderItem.Name
                Type = "Folder"
                LastModified = $folderLastModified
            }
        }

        return $result
    }
    catch {
        Write-Error "Failed to get items from source folder $FolderUrl : $_"
        return @()
    }
}

# ==========================================
# Function: Get Folder Items from Target (SPO)
# ==========================================
function Get-TargetFolderItems {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderUrl
    )

    try {
        $siteRelativePath = Get-SiteRelativePathFromUrl -Url $FolderUrl -SiteUrl $TargetSiteUrl

        if ([string]::IsNullOrWhiteSpace($siteRelativePath)) {
            throw "Could not derive site-relative folder path from target URL: $FolderUrl"
        }

        $files = $null
        $subFolders = $null

        try {
            $files = Get-PnPFolderItem -FolderSiteRelativeUrl $siteRelativePath -ItemType File -ErrorAction Stop
            $subFolders = Get-PnPFolderItem -FolderSiteRelativeUrl $siteRelativePath -ItemType Folder -ErrorAction Stop
        }
        catch {
            $errMsg = $_.Exception.Message
            if ($errMsg -like "*list view threshold*" -or $errMsg -like "*prohibited because it exceeds*" -or $errMsg -like "*attempted operation is prohibited*") {
                # Folder exceeds list view threshold -- fall back to paged retrieval
                return Get-TargetFolderItemsPaged -SiteRelativePath $siteRelativePath -FolderUrl $FolderUrl
            }

            # Some tenants/sites require encoded folder paths -- retry with encoding
            $encodedSiteRelativePath = [System.Uri]::EscapeUriString($siteRelativePath)
            try {
                $files = Get-PnPFolderItem -FolderSiteRelativeUrl $encodedSiteRelativePath -ItemType File -ErrorAction Stop
                $subFolders = Get-PnPFolderItem -FolderSiteRelativeUrl $encodedSiteRelativePath -ItemType Folder -ErrorAction Stop
            }
            catch {
                $errMsg2 = $_.Exception.Message
                if ($errMsg2 -like "*list view threshold*" -or $errMsg2 -like "*prohibited because it exceeds*" -or $errMsg2 -like "*attempted operation is prohibited*") {
                    return Get-TargetFolderItemsPaged -SiteRelativePath $encodedSiteRelativePath -FolderUrl $FolderUrl
                }
                throw
            }
        }
        
        $result = @()

        # Get files in folder
        foreach ($file in $files) {
            $fileLastModified = $null
            if ($file.PSObject.Properties.Name -contains "TimeLastModified") {
                $fileLastModified = $file.TimeLastModified
            }
            elseif ($file.PSObject.Properties.Name -contains "Modified") {
                $fileLastModified = $file.Modified
            }

            $result += @{
                Name = $file.Name
                Type = "File"
                LastModified = $fileLastModified
            }
        }

        # Get subfolders
        foreach ($subFolder in $subFolders) {
            if ($subFolder.Name -eq "Forms") {
                continue
            }

            $folderLastModified = $null
            if ($subFolder.PSObject.Properties.Name -contains "TimeLastModified") {
                $folderLastModified = $subFolder.TimeLastModified
            }
            elseif ($subFolder.PSObject.Properties.Name -contains "Modified") {
                $folderLastModified = $subFolder.Modified
            }

            $result += @{
                Name = $subFolder.Name
                Type = "Folder"
                LastModified = $folderLastModified
            }
        }

        return $result
    }
    catch {
        Write-Error "Failed to get items from target folder $FolderUrl : $_"
        return @()
    }
}

# ==========================================
# Function: Get Folder Items from Target (SPO) - Paged Fallback
# ==========================================
function Get-TargetFolderItemsPaged {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteRelativePath,

        [Parameter(Mandatory = $true)]
        [string]$FolderUrl
    )

    Write-Verbose "Using paged retrieval (list view threshold bypass) for: $FolderUrl"

    $serverRelativePath = Get-ServerRelativePathFromUrl -Url $FolderUrl -SiteUrl $TargetSiteUrl

    # Derive the library URL name -- first path segment after the site base path
    $siteUri = New-Object System.Uri($TargetSiteUrl)
    $sitePath = $siteUri.AbsolutePath.TrimEnd('/')
    $pathAfterSite = $serverRelativePath.Substring($sitePath.Length).TrimStart('/')
    $libraryUrlName = $pathAfterSite.Split('/')[0]

    # Get-PnPListItem with -FolderServerRelativeUrl and -PageSize bypasses the list view threshold.
    # If the library also exceeds the threshold, fall back to folder ItemCount comparison.
    $allItems = $null
    try {
        $allItems = Get-PnPListItem -List $libraryUrlName -FolderServerRelativeUrl $serverRelativePath -PageSize 500 -ErrorAction Stop
    }
    catch {
        $pagedErrMsg = $_.Exception.Message
        if ($pagedErrMsg -like "*list view threshold*" -or $pagedErrMsg -like "*prohibited because it exceeds*" -or $pagedErrMsg -like "*attempted operation is prohibited*") {
            Write-Warning "Get-TargetFolderItemsPaged: threshold also exceeded for $FolderUrl. Falling back to folder ItemCount comparison."
            $targetCount = -1
            try {
                $targetCount = Get-TargetFolderItemCount -FolderServerRelativePath $serverRelativePath
            }
            catch {
                $countErrMsg = $_.Exception.Message
                Write-Warning "Get-TargetFolderItemsPaged: ItemCount fetch failed for $FolderUrl. Error: $countErrMsg"
            }
            $script:ThresholdFolderCounts[$FolderUrl] = $targetCount
            return @()
        }
        throw
    }

    if ($null -eq $allItems) { $allItems = @() }
    $result = @()
    foreach ($item in $allItems) {
        $fsObjType = $item.FieldValues["FSObjType"]
        $name      = $item.FieldValues["FileLeafRef"]
        $modified  = $item.FieldValues["Modified"]

        if ($fsObjType -eq 0) {
            $result += @{ Name = $name; Type = "File"; LastModified = $modified }
        }
        elseif ($fsObjType -eq 1 -and $name -ne "Forms") {
            $result += @{ Name = $name; Type = "Folder"; LastModified = $modified }
        }
    }

    return $result
}

# ==========================================
# Function: Get Target Folder Item Count via REST
# ==========================================
function Get-TargetFolderItemCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativePath
    )

    $escapedPath = $FolderServerRelativePath.Replace("'", "''")
    $encodedPath = [System.Uri]::EscapeUriString($escapedPath)
    $baseApiUrl = $TargetSiteUrl.TrimEnd('/')
    $url = "$baseApiUrl/_api/web/GetFolderByServerRelativePath(decodedurl='$encodedPath')?`$select=ItemCount"

    Write-Verbose "Get-TargetFolderItemCount: url=$url"
    $rawResponse = Invoke-PnPSPRestMethod -Method Get -Url $url -ErrorAction Stop

    $json = $rawResponse
    if ($rawResponse -is [string]) {
        $json = $rawResponse | ConvertFrom-Json
    }

    $itemCount = -1
    if ($json.PSObject.Properties.Name -contains 'ItemCount') {
        $itemCount = [int]$json.ItemCount
    }
    elseif ($json.PSObject.Properties.Name -contains 'd') {
        if ($json.d.PSObject.Properties.Name -contains 'ItemCount') {
            $itemCount = [int]$json.d.ItemCount
        }
    }

    Write-Verbose "Get-TargetFolderItemCount: ItemCount=$itemCount"
    return $itemCount
}

# ==========================================
# Function: Compare Folder Pairs
# ==========================================
function Compare-FolderPair {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$TargetFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$InputFolderPath,

        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$SourceContext
    )

    Write-Verbose "Comparing: $SourceFolderUrl -> $TargetFolderUrl"

    # Get source items
    $sourceItems = Get-SourceFolderItems -FolderUrl $SourceFolderUrl -Context $SourceContext
    
    if ($null -eq $sourceItems -or $sourceItems.Count -eq 0) {
        Write-Verbose "Source folder is empty: $SourceFolderUrl"
        return @{
            Status = "Success"
            DifferenceItems = @()
            SourceItemCount = 0
        }
    }

    # Get target items
    $targetItems = Get-TargetFolderItems -FolderUrl $TargetFolderUrl

    if ($null -eq $targetItems) {
        $targetItems = @()
    }

    # If this folder hit the list view threshold, all enumeration returned empty.
    # Use the stored ItemCount for a count-only comparison instead.
    if ($script:ThresholdFolderCounts.ContainsKey($TargetFolderUrl)) {
        $targetItemCount = $script:ThresholdFolderCounts[$TargetFolderUrl]
        $sourceCount = $sourceItems.Count
        $itemDiff = -1
        $countStatus = 'Error'
        if ($targetItemCount -ge 0) {
            $itemDiff = $sourceCount - $targetItemCount
            if ($itemDiff -eq 0) { $countStatus = 'Match' } else { $countStatus = 'CountMismatch' }
        }

        Write-Warning "Compare-FolderPair: count-only comparison for '$InputFolderPath'. Source=$sourceCount Target=$targetItemCount Status=$countStatus"

        $script:FolderCountLog += [PSCustomObject]@{
            Timestamp        = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
            SourceFolderPath = $InputFolderPath
            TargetFolderPath = $InputFolderPath
            SourceItemCount  = $sourceCount
            TargetItemCount  = $targetItemCount
            Difference       = $itemDiff
            Status           = $countStatus
        }

        $countDiffItems = @()
        if ($countStatus -ne 'Match') {
            $countDiffItems += @{
                Name            = '(ItemCount)'
                Type            = 'CountMismatch'
                LastModified    = $null
                SourceUrl       = $SourceFolderUrl
                TargetUrl       = $TargetFolderUrl
                InputFolderPath = $InputFolderPath
                Status          = "CountMismatch: Source=$sourceCount Target=$targetItemCount"
            }
        }

        return @{
            Status          = 'Success'
            DifferenceItems = $countDiffItems
            SourceItemCount = $sourceCount
        }
    }

    # Create lookup table for faster comparison
    $targetItemLookup = @{}
    foreach ($item in $targetItems) {
        $key = $item.Name.ToLowerInvariant()
        $targetItemLookup[$key] = $item
    }

    $sourceItemLookup = @{}
    foreach ($item in $sourceItems) {
        $key = $item.Name.ToLowerInvariant()
        $sourceItemLookup[$key] = $item
    }

    # Find missing items in target
    $differenceItems = @()
    foreach ($sourceItem in $sourceItems) {
        $key = $sourceItem.Name.ToLowerInvariant()
        if (-not $targetItemLookup.ContainsKey($key)) {
            $differenceItems += @{
                Name = $sourceItem.Name
                Type = $sourceItem.Type
                LastModified = $sourceItem.LastModified
                SourceUrl = "$SourceFolderUrl/$($sourceItem.Name)"
                TargetUrl = "$TargetFolderUrl/$($sourceItem.Name)"
                InputFolderPath = $InputFolderPath
                Status = "Missing in Target"
            }
        }
    }

    # Find extra items in target
    foreach ($targetItem in $targetItems) {
        $key = $targetItem.Name.ToLowerInvariant()
        if (-not $sourceItemLookup.ContainsKey($key)) {
            $differenceItems += @{
                Name = $targetItem.Name
                Type = $targetItem.Type
                LastModified = $targetItem.LastModified
                SourceUrl = "$SourceFolderUrl/$($targetItem.Name)"
                TargetUrl = "$TargetFolderUrl/$($targetItem.Name)"
                InputFolderPath = $InputFolderPath
                Status = "Extra in Target"
            }
        }
    }

    return @{
        Status = "Success"
        DifferenceItems = $differenceItems
        SourceItemCount = $sourceItems.Count
    }
}

# ==========================================
# Function: Add Difference Item to Log (Thread-Safe)
# ==========================================
function Add-DifferenceItemToLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$MissingItem,

        [Parameter(Mandatory = $true)]
        [string]$SourceFolderUrl,

        [Parameter(Mandatory = $true)]
        [string]$TargetFolderUrl
    )

    $null = $script:LogLock.WaitOne()
    try {
        $script:MissingItems += [PSCustomObject]@{
            Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            InputFolderPath = $MissingItem.InputFolderPath
            ItemName = $MissingItem.Name
            ItemType = $MissingItem.Type
            SourceUrl = $MissingItem.SourceUrl
            TargetUrl = $MissingItem.TargetUrl
            LastModifiedDate = $MissingItem.LastModified
            Status = $MissingItem.Status
        }
        $script:TotalDifferences++

        if ($MissingItem.Status -eq "Missing in Target") {
            $script:TotalMissingInTarget++
        }
        elseif ($MissingItem.Status -eq "Extra in Target") {
            $script:TotalExtraInTarget++
        }

        # Export if reached max records per file
        if ($script:MissingItems.Count -ge $MaxRecordsPerFile) {
            Export-MissingItemsToFile
        }
    }
    finally {
        $script:LogLock.ReleaseMutex()
    }
}

# ==========================================
# Function: Export Missing Items to CSV
# ==========================================
function Export-MissingItemsToFile {
    [CmdletBinding()]
    param()

    if ($script:MissingItems.Count -eq 0) {
        return
    }

    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $outputFile = Join-Path $OutputFolder "MissingItems_Part$($script:OutputFileCounter)_$timestamp.csv"

    try {
        $script:MissingItems | Export-Csv -Path $outputFile -NoTypeInformation -Encoding UTF8 -Force
        Write-Host "[OK] Exported $($script:MissingItems.Count) items to $outputFile"
        $script:OutputFileCounter++
        $script:MissingItems = @()
    }
    catch {
        Write-Error "Failed to export missing items: $_"
    }
}

# ==========================================
# Function: Export Folder Item Count Log
# ==========================================
function Export-FolderCountLog {
    [CmdletBinding()]
    param()

    if ($script:FolderCountLog.Count -eq 0) {
        return
    }

    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $outputFile = Join-Path $OutputFolder "FolderItemCounts_$timestamp.csv"

    try {
        $script:FolderCountLog | Export-Csv -Path $outputFile -NoTypeInformation -Encoding UTF8 -Force
        Write-Host "[OK] Exported $($script:FolderCountLog.Count) threshold folder records to $outputFile"
    }
    catch {
        Write-Error "Failed to export folder count log: $_"
    }
}

# ==========================================
# Function: Process Folder Pair (Job Item)
# ==========================================
function Invoke-FolderComparison {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SourceUrl,

        [Parameter(Mandatory = $true)]
        [string]$TargetUrl,

        [Parameter(Mandatory = $false)]
        [string]$InputFolderPath = ""
    )

    try {
        $sourceContext = Get-SourceContext -SiteUrl $SourceSiteUrl
        if ($null -eq $sourceContext) {
            $script:FailedFolders++
            return
        }

        # Compare folders
        $result = Compare-FolderPair -SourceFolderUrl $SourceUrl -TargetFolderUrl $TargetUrl -InputFolderPath $InputFolderPath -SourceContext $sourceContext

        if ($result.Status -eq "Success") {
            $null = $script:LogLock.WaitOne()
            try {
                $script:ProcessedFolders++
                
                # Log differences
                foreach ($differenceItem in $result.DifferenceItems) {
                    Add-DifferenceItemToLog -MissingItem $differenceItem -SourceFolderUrl $SourceUrl -TargetFolderUrl $TargetUrl
                }
            }
            finally {
                $script:LogLock.ReleaseMutex()
            }
        }
        else {
            $script:FailedFolders++
        }

        $diffCount = $result.DifferenceItems.Count
        Write-Host "Processed: $SourceUrl ($diffCount differences)"
    }
    catch {
        Write-Error "Error processing pair $SourceUrl <-> $TargetUrl : $_"
        $script:FailedFolders++
    }
}

# ==========================================
# Function: Main Execution
# ==========================================
function Invoke-Migration {
    [CmdletBinding()]
    param()

    Write-Host "Starting folder migration comparison..." -ForegroundColor Cyan

    # Validate CSV input
    if (-not (Test-Path $CsvInputPath)) {
        throw "CSV input file not found: $CsvInputPath"
    }

    # Import CSOM assemblies
    Import-CSOMAssemblies

    # Initialize target connection
    Initialize-TargetConnection

    # Read and resolve CSV rows
    $folderPairs = @()
    try {
        $csvRows = Import-Csv -Path $CsvInputPath -ErrorAction Stop
        foreach ($row in $csvRows) {
            $folderPairs += Resolve-FolderPair -Row $row
        }
        Write-Host "Loaded $($folderPairs.Count) folder pairs from CSV" -ForegroundColor Green
    }
    catch {
        throw "Failed to read CSV file: $_"
    }

    if ($folderPairs.Count -eq 0) {
        throw "No folder pairs found in CSV"
    }

    # Process folder pairs using runspace pool for parallelization
    $startTime = Get-Date
    $processedCount = 0

    if ($ThreadCount -gt 1) {
        # Use runspace pool for multi-threaded execution
        $runspacePool = [runspacefactory]::CreateRunspacePool(1, $ThreadCount)
        $runspacePool.Open()
        $jobs = @()

        foreach ($pair in $folderPairs) {
            $ps = [powershell]::Create().AddScript(${function:Invoke-FolderComparison}).AddParameter("SourceUrl", $pair.SourceFolderUrl).AddParameter("TargetUrl", $pair.TargetFolderUrl).AddParameter("InputFolderPath", $pair.FolderPath)
            $ps.RunspacePool = $runspacePool
            $jobs += @{
                Pipe = $ps
                Handle = $ps.BeginInvoke()
                SourceUrl = $pair.SourceFolderUrl
            }
        }

        # Wait for all jobs to complete
        foreach ($job in $jobs) {
            try {
                $job.Pipe.EndInvoke($job.Handle)
            }
            catch {
                Write-Error "Job failed for $($job.SourceUrl): $_"
            }
            finally {
                $job.Pipe.Dispose()
            }
        }

        $runspacePool.Close()
        $runspacePool.Dispose()
    }
    else {
        # Single-threaded execution
        foreach ($pair in $folderPairs) {
            Invoke-FolderComparison -SourceUrl $pair.SourceFolderUrl -TargetUrl $pair.TargetFolderUrl -InputFolderPath $pair.FolderPath
            $processedCount++
            if ($processedCount % 10 -eq 0) {
                Write-Progress -Activity "Processing folder pairs" -CurrentOperation "$processedCount/$($folderPairs.Count)" -PercentComplete (($processedCount / $folderPairs.Count) * 100)
            }
        }
    }

    # Export remaining items
    if ($script:MissingItems.Count -gt 0) {
        Export-MissingItemsToFile
    }

    # Export folder item count log (folders that hit list view threshold)
    if ($script:FolderCountLog.Count -gt 0) {
        Export-FolderCountLog
    }

    # Generate summary report
    $endTime = Get-Date
    $duration = $endTime - $startTime

    $summaryReport = @{
        ExecutionTime = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalFolderPairs = $folderPairs.Count
        ProcessedFolders = $script:ProcessedFolders
        FailedFolders = $script:FailedFolders
        TotalDifferences = $script:TotalDifferences
        MissingInTarget = $script:TotalMissingInTarget
        ExtraInTarget = $script:TotalExtraInTarget
        OutputFiles = $script:OutputFileCounter - 1
        TimestampStart = $startTime
        TimestampEnd = $endTime
    }

    # Export summary
    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"
    $summaryReport | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8
    
    # Display summary
    Write-Host "`n" -ForegroundColor Cyan
    Write-Host "================================" -ForegroundColor Cyan
    Write-Host "Migration Comparison Summary" -ForegroundColor Cyan
    Write-Host "================================" -ForegroundColor Cyan
    Write-Host "Total Folder Pairs:    $($summaryReport.TotalFolderPairs)"
    Write-Host "Successfully Processed: $($summaryReport.ProcessedFolders)" -ForegroundColor Green
    Write-Host "Failed:                $($summaryReport.FailedFolders)" -ForegroundColor $(if ($summaryReport.FailedFolders -gt 0) { "Red" } else { "Green" })
    Write-Host "Missing In Target:     $($summaryReport.MissingInTarget)" -ForegroundColor $(if ($summaryReport.MissingInTarget -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Extra In Target:       $($summaryReport.ExtraInTarget)" -ForegroundColor $(if ($summaryReport.ExtraInTarget -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Total Differences:     $($summaryReport.TotalDifferences)" -ForegroundColor $(if ($summaryReport.TotalDifferences -gt 0) { "Yellow" } else { "Green" })
    Write-Host "Output Files Created:  $($summaryReport.OutputFiles)"
    Write-Host "Execution Time:        $($summaryReport.ExecutionTime)"
    Write-Host "Output Location:       $OutputFolder"
    Write-Host "================================" -ForegroundColor Cyan
}

# ==========================================
# Script Entry Point
# ==========================================
try {
    Invoke-Migration
    Write-Host "`n[OK] Migration comparison completed successfully" -ForegroundColor Green
}
catch {
    Write-Host "`n[FAIL] Migration comparison failed: $_" -ForegroundColor Red
    exit 1
}

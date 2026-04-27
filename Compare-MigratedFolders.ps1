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
        Write-Host "Connecting to SharePoint Online..."
        Write-Verbose "Initialize-TargetConnection: TargetSiteUrl=$TargetSiteUrl, TargetUsername=$TargetUsername"
        $securePassword = ConvertTo-SecureString $TargetPassword -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential($TargetUsername, $securePassword)

        Connect-PnPOnline -Url $TargetSiteUrl -Credentials $credential -ClientId $ClientId -ErrorAction Stop
        $script:TargetContext = Get-PnPConnection
        if ($null -eq $script:TargetContext) {
            Write-Warning "Initialize-TargetConnection: Get-PnPConnection returned null after connect."
        }
        else {
            Write-Verbose "Initialize-TargetConnection: PnP connection established successfully."
        }
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

function Test-IsListViewThresholdError {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$ErrorRecord
    )

    $message = ""
    if ($null -ne $ErrorRecord.Exception -and -not [string]::IsNullOrWhiteSpace($ErrorRecord.Exception.Message)) {
        $message = $ErrorRecord.Exception.Message
    }

    $lowerMessage = $message.ToLowerInvariant()
    $serverTypeName = ""
    if ($null -ne $ErrorRecord.Exception -and $null -ne $ErrorRecord.Exception.ServerErrorTypeName) {
        $serverTypeName = [string]$ErrorRecord.Exception.ServerErrorTypeName
    }

    if ($lowerMessage -like "*list*view*threshold*" -or
        $lowerMessage -like "*attempted operation is prohibited*" -or
        $lowerMessage -like "*prohibited because it exceeds*") {
        return $true
    }

    if ($serverTypeName -eq "Microsoft.SharePoint.SPQueryThrottledException") {
        return $true
    }

    return $false
}

function Get-LibraryIdentifierFromServerRelativePath {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ServerRelativePath,

        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    $siteUri = New-Object System.Uri($SiteUrl)
    $sitePath = $siteUri.AbsolutePath.TrimEnd('/')

    $pathAfterSite = $ServerRelativePath.TrimStart('/')
    if ((-not [string]::IsNullOrWhiteSpace($sitePath)) -and ($sitePath -ne '/') -and $ServerRelativePath.StartsWith($sitePath, [System.StringComparison]::OrdinalIgnoreCase)) {
        $pathAfterSite = $ServerRelativePath.Substring($sitePath.Length).TrimStart('/')
    }

    if ([string]::IsNullOrWhiteSpace($pathAfterSite)) {
        throw "Could not derive library path from folder URL: $ServerRelativePath"
    }

    $librarySegment = $pathAfterSite.Split('/')[0]
    if ([string]::IsNullOrWhiteSpace($librarySegment)) {
        throw "Could not derive library segment from folder URL: $ServerRelativePath"
    }

    return [System.Uri]::UnescapeDataString($librarySegment)
}

function Resolve-TargetListForFolder {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativePath,

        [Parameter(Mandatory = $true)]
        [string]$SiteUrl
    )

    $libraryUrlName = Get-LibraryIdentifierFromServerRelativePath -ServerRelativePath $FolderServerRelativePath -SiteUrl $SiteUrl
    $sitePath = (New-Object System.Uri($SiteUrl)).AbsolutePath.TrimEnd('/')
    $libraryServerRelativePath = "$sitePath/$libraryUrlName"

    $lists = Get-PnPList -Includes RootFolder,Title,Id -ErrorAction Stop
    foreach ($candidateList in $lists) {
        $candidateRootUrl = [string]$candidateList.RootFolder.ServerRelativeUrl
        if (-not [string]::IsNullOrWhiteSpace($candidateRootUrl)) {
            $decodedCandidateRootUrl = [System.Uri]::UnescapeDataString($candidateRootUrl)
            if ($decodedCandidateRootUrl.Equals($libraryServerRelativePath, [System.StringComparison]::OrdinalIgnoreCase)) {
                return $candidateList
            }
        }

        if ($candidateList.Title -eq $libraryUrlName) {
            return $candidateList
        }
    }

    throw "Could not resolve target library for folder path: $FolderServerRelativePath"
}

function Get-TargetFolderItemsByCaml {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Guid]$ListId,

        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativePath
    )

    Write-Verbose "Get-TargetFolderItemsByCaml: listId=$ListId, folderServerRelativePath=$FolderServerRelativePath"

    $escapedFolderPath = [System.Security.SecurityElement]::Escape($FolderServerRelativePath)

    $query = @"
<View>
  <Query>
    <Where>
      <Eq>
        <FieldRef Name='FileDirRef' />
        <Value Type='Text'>$escapedFolderPath</Value>
      </Eq>
    </Where>
  </Query>
  <RowLimit Paged='TRUE'>500</RowLimit>
</View>
"@

    $items = @(Get-PnPListItem -List $ListId -Query $query -PageSize 500 -ErrorAction Stop)
    $result = @()

    foreach ($item in $items) {
        $fsObjType = $item.FieldValues["FSObjType"]
        $name = [string]$item.FieldValues["FileLeafRef"]
        $modified = $item.FieldValues["Modified"]

        if ([string]::IsNullOrWhiteSpace($name)) {
            continue
        }

        if ($fsObjType -eq 0) {
            $result += @{ Name = $name; Type = "File"; LastModified = $modified }
        }
        elseif ($fsObjType -eq 1 -and $name -ne "Forms") {
            $result += @{ Name = $name; Type = "Folder"; LastModified = $modified }
        }
    }

    Write-Verbose "Get-TargetFolderItemsByCaml: retrieved $(@($result).Count) items"

    return @($result)
}

function ConvertFrom-PnPRestResponse {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Response
    )

    if ($Response -is [string]) {
        return ($Response | ConvertFrom-Json)
    }

    return $Response
}

function Get-ObjectPropertyValue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$InputObject,

        [Parameter(Mandatory = $true)]
        [string[]]$PropertyNames
    )

    foreach ($propertyName in $PropertyNames) {
        $property = $InputObject.PSObject.Properties[$propertyName]
        if ($null -ne $property) {
            return $property.Value
        }
    }

    return $null
}

function Convert-RenderListDataRowsToTargetItems {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Rows
    )

    $result = @()
    foreach ($row in $Rows) {
        $name = [string](Get-ObjectPropertyValue -InputObject $row -PropertyNames @('FileLeafRef', 'FileLeafRef.Name', 'FileName'))
        $modified = Get-ObjectPropertyValue -InputObject $row -PropertyNames @('Modified', 'Modified.', 'TimeLastModified')
        $fsObjTypeValue = Get-ObjectPropertyValue -InputObject $row -PropertyNames @('FSObjType', '.FSObjType')
        $contentType = [string](Get-ObjectPropertyValue -InputObject $row -PropertyNames @('ContentType', 'ContentTypeId'))

        if ([string]::IsNullOrWhiteSpace($name) -or $name -eq 'Forms') {
            continue
        }

        $itemType = ''
        if (($null -ne $fsObjTypeValue) -and ([string]$fsObjTypeValue -eq '1')) {
            $itemType = 'Folder'
        }
        elseif (($null -ne $fsObjTypeValue) -and ([string]$fsObjTypeValue -eq '0')) {
            $itemType = 'File'
        }
        elseif ($contentType -like '0x0120*') {
            $itemType = 'Folder'
        }
        else {
            $itemType = 'File'
        }

        $result += @{
            Name = $name
            Type = $itemType
            LastModified = $modified
        }
    }

    return @($result)
}

function Get-TargetFolderItemsByRenderListData {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ListServerRelativePath,

        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativePath
    )

    Write-Verbose "Get-TargetFolderItemsByRenderListData: listServerRelativePath=$ListServerRelativePath, folderServerRelativePath=$FolderServerRelativePath"

    $baseApiUrl = $TargetSiteUrl.TrimEnd('/')
    $encodedListPath = [System.Uri]::EscapeUriString($ListServerRelativePath)
    $endpoint = "$baseApiUrl/_api/web/GetList(@listUrl)/RenderListDataAsStream?@listUrl='$encodedListPath'"
    $rows = @()
    $paging = $null

    do {
        $parameters = @{
            RenderOptions = 2
            FolderServerRelativeUrl = $FolderServerRelativePath
            ViewXml = '<View Scope="Default"><RowLimit Paged="TRUE">500</RowLimit></View>'
        }

        if (-not [string]::IsNullOrWhiteSpace($paging)) {
            $parameters.Paging = $paging
        }

        $bodyObject = @{ parameters = $parameters }
        $body = $bodyObject | ConvertTo-Json -Depth 10

        Write-Verbose "Get-TargetFolderItemsByRenderListData: requesting page with paging token present=$(-not [string]::IsNullOrWhiteSpace($paging))"
        $rawResponse = Invoke-PnPSPRestMethod -Method Post -Url $endpoint -Content $body -ContentType 'application/json;odata=nometadata' -ErrorAction Stop
        $json = ConvertFrom-PnPRestResponse -Response $rawResponse

        $pageRows = @()
        if ($json.PSObject.Properties.Name -contains 'Row') {
            $pageRows = @($json.Row)
        }
        elseif (($json.PSObject.Properties.Name -contains 'ListData') -and ($json.ListData.PSObject.Properties.Name -contains 'Row')) {
            $pageRows = @($json.ListData.Row)
        }

        $rows += $pageRows
        $paging = $null

        if ($json.PSObject.Properties.Name -contains 'NextHref') {
            $paging = [string]$json.NextHref
        }
        elseif (($json.PSObject.Properties.Name -contains 'ListData') -and ($json.ListData.PSObject.Properties.Name -contains 'NextHref')) {
            $paging = [string]$json.ListData.NextHref
        }
    }
    while (-not [string]::IsNullOrWhiteSpace($paging))

    Write-Verbose "Get-TargetFolderItemsByRenderListData: raw rows returned=$($rows.Count)"
    $result = Convert-RenderListDataRowsToTargetItems -Rows $rows
    Write-Verbose "Get-TargetFolderItemsByRenderListData: normalized target item count=$($result.Count)"
    return @($result)
}

function Get-PnPRestCollectionItems {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$RequestUrl
    )

    $allItems = @()
    $nextUrl = $RequestUrl

    while (-not [string]::IsNullOrWhiteSpace($nextUrl)) {
        Write-Verbose "Get-PnPRestCollectionItems: requesting $nextUrl"

        $rawResponse = Invoke-PnPSPRestMethod -Method Get -Url $nextUrl -ErrorAction Stop
        $json = ConvertFrom-PnPRestResponse -Response $rawResponse
        $pageItems = @()
        $nextLink = $null

        if ($json.PSObject.Properties.Name -contains 'd') {
            if ($json.d.PSObject.Properties.Name -contains 'results') {
                $pageItems = @($json.d.results)
            }
            else {
                $pageItems = @($json.d)
            }

            if ($json.d.PSObject.Properties.Name -contains '__next') {
                $nextLink = [string]$json.d.__next
            }
        }
        else {
            if ($json.PSObject.Properties.Name -contains 'value') {
                $pageItems = @($json.value)
            }
            else {
                $pageItems = @($json)
            }

            if ($json.PSObject.Properties.Name -contains '@odata.nextLink') {
                $nextLink = [string]$json.'@odata.nextLink'
            }
            elseif ($json.PSObject.Properties.Name -contains 'odata.nextLink') {
                $nextLink = [string]$json.'odata.nextLink'
            }
        }

        $allItems += $pageItems

        $nextUrl = $nextLink
    }

    return @($allItems)
}

function Get-TargetFolderItemsByRest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativePath
    )

    Write-Verbose "Get-TargetFolderItemsByRest: folderServerRelativePath=$FolderServerRelativePath"

    $escapedFolderPath = $FolderServerRelativePath.Replace("'", "''")
    $encodedFolderPath = [System.Uri]::EscapeUriString($escapedFolderPath)
    $baseApiUrl = $TargetSiteUrl.TrimEnd('/')

    $filesUrl = "$baseApiUrl/_api/web/GetFolderByServerRelativePath(decodedurl='$encodedFolderPath')/Files?`$select=Name,TimeLastModified,ServerRelativeUrl&`$top=5000"
    $foldersUrl = "$baseApiUrl/_api/web/GetFolderByServerRelativePath(decodedurl='$encodedFolderPath')/Folders?`$select=Name,TimeLastModified,ServerRelativeUrl&`$top=5000"

    Write-Verbose "Get-TargetFolderItemsByRest: filesUrl=$filesUrl"
    Write-Verbose "Get-TargetFolderItemsByRest: foldersUrl=$foldersUrl"

    $files = @(Get-PnPRestCollectionItems -RequestUrl $filesUrl)
    $folders = @(Get-PnPRestCollectionItems -RequestUrl $foldersUrl)

    Write-Verbose "Get-TargetFolderItemsByRest: files=$($files.Count), folders=$($folders.Count)"

    $result = @()

    foreach ($file in $files) {
        $fileName = [string]$file.Name
        if ([string]::IsNullOrWhiteSpace($fileName)) {
            continue
        }

        $result += @{
            Name = $fileName
            Type = 'File'
            LastModified = $file.TimeLastModified
        }
    }

    foreach ($folder in $folders) {
        $folderName = [string]$folder.Name
        if ([string]::IsNullOrWhiteSpace($folderName) -or $folderName -eq 'Forms') {
            continue
        }

        $result += @{
            Name = $folderName
            Type = 'Folder'
            LastModified = $folder.TimeLastModified
        }
    }

    Write-Verbose "Get-TargetFolderItemsByRest: normalized target item count=$(@($result).Count)"
    return @($result)
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
        Write-Verbose "Get-TargetFolderItems: folderUrl=$FolderUrl"
        Write-Verbose "Get-TargetFolderItems: siteRelativePath=$siteRelativePath"

        $currentPnPConnection = Get-PnPConnection -ErrorAction SilentlyContinue
        if ($null -eq $currentPnPConnection) {
            Write-Warning "Get-TargetFolderItems: No active PnP connection detected before query."
        }
        else {
            Write-Verbose "Get-TargetFolderItems: Active PnP connection detected."
        }

        if ([string]::IsNullOrWhiteSpace($siteRelativePath)) {
            throw "Could not derive site-relative folder path from target URL: $FolderUrl"
        }

        $files = $null
        $subFolders = $null

        try {
            Write-Verbose "Get-TargetFolderItems: querying target via Get-PnPFolderItem (siteRelativePath)"
            $files = Get-PnPFolderItem -FolderSiteRelativeUrl $siteRelativePath -ItemType File -ErrorAction Stop
            $subFolders = Get-PnPFolderItem -FolderSiteRelativeUrl $siteRelativePath -ItemType Folder -ErrorAction Stop
            $fileCount = 0
            $folderCount = 0
            if ($null -ne $files) { $fileCount = $files.Count }
            if ($null -ne $subFolders) { $folderCount = $subFolders.Count }
            Write-Verbose "Get-TargetFolderItems: Get-PnPFolderItem returned files=$fileCount, folders=$folderCount"
        }
        catch {
            if (Test-IsListViewThresholdError -ErrorRecord $_) {
                # Folder exceeds list view threshold - fall back to paged retrieval
                Write-Warning "Get-TargetFolderItems: threshold detected for $FolderUrl. Switching to paged fallback."
                return Get-TargetFolderItemsPaged -SiteRelativePath $siteRelativePath -FolderUrl $FolderUrl
            }

            # Some tenants/sites require encoded folder paths - retry with encoding
            $encodedSiteRelativePath = [System.Uri]::EscapeUriString($siteRelativePath)
            Write-Verbose "Get-TargetFolderItems: retrying Get-PnPFolderItem with encoded path=$encodedSiteRelativePath"
            try {
                $files = Get-PnPFolderItem -FolderSiteRelativeUrl $encodedSiteRelativePath -ItemType File -ErrorAction Stop
                $subFolders = Get-PnPFolderItem -FolderSiteRelativeUrl $encodedSiteRelativePath -ItemType Folder -ErrorAction Stop
                $encodedFileCount = 0
                $encodedFolderCount = 0
                if ($null -ne $files) { $encodedFileCount = $files.Count }
                if ($null -ne $subFolders) { $encodedFolderCount = $subFolders.Count }
                Write-Verbose "Get-TargetFolderItems: encoded Get-PnPFolderItem returned files=$encodedFileCount, folders=$encodedFolderCount"
            }
            catch {
                if (Test-IsListViewThresholdError -ErrorRecord $_) {
                    Write-Warning "Get-TargetFolderItems: threshold detected on encoded path for $FolderUrl. Switching to paged fallback."
                    return Get-TargetFolderItemsPaged -SiteRelativePath $encodedSiteRelativePath -FolderUrl $FolderUrl
                }
                Write-Warning "Get-TargetFolderItems: encoded path query failed for $FolderUrl. Error: $($_.Exception.Message)"
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

        Write-Verbose "Get-TargetFolderItems: normalized target item count=$($result.Count)"

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

    $decodedServerRelativePath = [System.Uri]::UnescapeDataString($serverRelativePath)
    $targetList = $null
    try {
        $targetList = Resolve-TargetListForFolder -FolderServerRelativePath $decodedServerRelativePath -SiteUrl $TargetSiteUrl
        Write-Verbose "Get-TargetFolderItemsPaged: resolved target list id=$($targetList.Id), title=$($targetList.Title)"

        # Use list item paging scoped to the folder path for broad module compatibility.
        $pagedItems = @(Get-PnPListItem -List $targetList.Id -FolderServerRelativeUrl $decodedServerRelativePath -PageSize 500 -ErrorAction Stop)
        Write-Verbose "Get-TargetFolderItemsPaged: FolderServerRelativeUrl query returned $($pagedItems.Count) raw items"

        $result = @()
        foreach ($item in $pagedItems) {
            $fsObjType = $item.FieldValues["FSObjType"]
            $name = [string]$item.FieldValues["FileLeafRef"]
            $modified = $item.FieldValues["Modified"]

            if ([string]::IsNullOrWhiteSpace($name)) {
                continue
            }

            if ($fsObjType -eq 0) {
                $result += @{ Name = $name; Type = "File"; LastModified = $modified }
            }
            elseif ($fsObjType -eq 1 -and $name -ne "Forms") {
                $result += @{ Name = $name; Type = "Folder"; LastModified = $modified }
            }
        }

        Write-Verbose "Get-TargetFolderItemsPaged: normalized target item count=$(@($result).Count)"

        return @($result)
    }
    catch {
        Write-Warning "Get-TargetFolderItemsPaged: FolderServerRelativeUrl retrieval failed for $FolderUrl. Error: $($_.Exception.Message). Retrying with CAML paging."

        if ($null -eq $targetList) {
            $targetList = Resolve-TargetListForFolder -FolderServerRelativePath $decodedServerRelativePath -SiteUrl $TargetSiteUrl
            Write-Verbose "Get-TargetFolderItemsPaged: resolved target list for CAML fallback id=$($targetList.Id), title=$($targetList.Title)"
        }

        try {
            $camlItems = @(Get-TargetFolderItemsByCaml -ListId $targetList.Id -FolderServerRelativePath $decodedServerRelativePath)
            Write-Verbose "Get-TargetFolderItemsPaged: CAML fallback returned $($camlItems.Count) items"

            if ($camlItems.Count -gt 0) {
                return @($camlItems)
            }

            Write-Warning "Get-TargetFolderItemsPaged: CAML fallback returned zero items for $FolderUrl. Retrying with REST folder enumeration."
        }
        catch {
            Write-Warning "Get-TargetFolderItemsPaged: CAML fallback failed for $FolderUrl. Error: $($_.Exception.Message). Retrying with REST folder enumeration."
        }

        try {
            $renderItems = @(Get-TargetFolderItemsByRenderListData -ListServerRelativePath $targetList.RootFolder.ServerRelativeUrl -FolderServerRelativePath $decodedServerRelativePath)
            Write-Verbose "Get-TargetFolderItemsPaged: RenderListData fallback returned $($renderItems.Count) items"

            if ($renderItems.Count -gt 0) {
                return @($renderItems)
            }

            Write-Warning "Get-TargetFolderItemsPaged: RenderListData fallback returned zero items for $FolderUrl. Retrying with REST folder enumeration."
        }
        catch {
            Write-Warning "Get-TargetFolderItemsPaged: RenderListData fallback failed for $FolderUrl. Error: $($_.Exception.Message). Retrying with REST folder enumeration."
        }

        $restItems = @(Get-TargetFolderItemsByRest -FolderServerRelativePath $decodedServerRelativePath)
        Write-Verbose "Get-TargetFolderItemsPaged: REST fallback returned $($restItems.Count) items"
        return @($restItems)
    }
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

    Write-Verbose "Compare-FolderPair: sourceItems=$($sourceItems.Count), targetItems=$($targetItems.Count), folder=$InputFolderPath"
    if (($sourceItems.Count -gt 0) -and ($targetItems.Count -eq 0)) {
        Write-Warning "Compare-FolderPair: target returned zero items while source has $($sourceItems.Count) items for folder: $InputFolderPath"
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

        Write-Host ("Processed: {0} ({1} differences)" -f $SourceUrl, $result.DifferenceItems.Count)
    }
    catch {
        Write-Error ("Error processing pair {0} to {1} : {2}" -f $SourceUrl, $TargetUrl, $_)
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
    $ThreadCount=0
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

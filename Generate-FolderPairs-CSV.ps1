#Requires -Version 5.1
<#
.SYNOPSIS
    Generates folder-pair CSV rows from SharePoint Online document libraries.

.DESCRIPTION
    Reads a CSV file with SharePoint Online site URLs and library names, then enumerates
    all nested folders from each library using PnP PowerShell.

    Input CSV supports an additional FolderName column.
    - If FolderName has a value, only that root-level folder and its nested folders are queried.
    - If FolderName is empty, all folders in the library are queried.

    Output is generated in two files:
    1) FolderPairs_*.csv with TargetFolderUrl and ItemsCount columns.
    2) Results_*.csv with per-row processing status.

    A Summary_*.json file is also generated.

.PARAMETER CsvInputPath
    Path to CSV file with required columns: SiteUrl, LibraryName, FolderName.

.PARAMETER ClientId
    Azure AD app client ID for SPO PnP authentication.

.PARAMETER TargetUsername
    SPO username for authentication (hardcoded credential).

.PARAMETER TargetPassword
    SPO password for authentication (hardcoded credential).

.PARAMETER QueryPageSize
    Page size for list item retrieval.
    Default: 2000

.PARAMETER MaxRetryCount
    Maximum retries for throttled CSOM ExecuteQuery calls (HTTP 429/503).
    Default: 8

.PARAMETER RetryBaseDelaySeconds
    Base backoff delay in seconds for throttled requests.
    Default: 2

.PARAMETER OutputFolder
    Folder path for output files.
    Default: ./GenerateFolderPairsLog-{timestamp}

.EXAMPLE
    .\Generate-FolderPairs-CSV.ps1 `
        -CsvInputPath ".\LibraryFolders-Template.csv" `
        -ClientId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -TargetUsername "admin@tenant.onmicrosoft.com" `
        -TargetPassword "Password123!"
#>

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

    [int]$QueryPageSize = 2000,

    [int]$MaxRetryCount = 8,

    [int]$RetryBaseDelaySeconds = 2,

    [string]$OutputFolder = "./GenerateFolderPairsLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:Results = @()
$script:FolderPairs = @()
$script:ProcessedRows = 0
$script:SuccessfulRows = 0
$script:FailedRows = 0
$script:TotalRows = 0
$script:Credential = $null
$script:TotalFoldersDiscovered = 0

if (-not (Test-Path $CsvInputPath)) {
    throw "CSV input file not found: $CsvInputPath"
}

if ($QueryPageSize -le 0) {
    throw "QueryPageSize must be greater than 0."
}

if ($MaxRetryCount -lt 0) {
    throw "MaxRetryCount must be 0 or greater."
}

if ($RetryBaseDelaySeconds -le 0) {
    throw "RetryBaseDelaySeconds must be greater than 0."
}

if (-not (Test-Path $OutputFolder)) {
    New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
    Write-Host "Created output folder: $OutputFolder"
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

    try {
        Write-Verbose "Connecting to $SiteUrl"
        Connect-PnPOnline -Url $SiteUrl -Credentials $script:Credential -ClientId $ClientId -ErrorAction Stop
        Write-Host "Connected: $SiteUrl"
    }
    catch {
        throw "Failed to connect to site $SiteUrl : $_"
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

    $first = $Rows[0]
    $requiredColumns = @("SiteUrl", "LibraryName", "FolderName")

    foreach ($column in $requiredColumns) {
        if (-not ($first.PSObject.Properties.Name -contains $column)) {
            throw "CSV must contain required column: $column"
        }
    }
}

# ==========================================
# Function: Disconnect SPO Connection
# ==========================================
function Disconnect-PnPIfConnected {
    [CmdletBinding()]
    param()

    try {
        $existingConnection = Get-PnPConnection -ErrorAction SilentlyContinue
        if ($null -ne $existingConnection) {
            Disconnect-PnPOnline -ErrorAction SilentlyContinue
        }
    }
    catch {
        # Intentionally suppress disconnect errors when no active connection exists.
    }
}

# ==========================================
# Function: Execute CSOM Query with Retry
# ==========================================
function Invoke-CSOMExecuteQueryWithRetry {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$ClientContext,

        [Parameter(Mandatory = $true)]
        [string]$OperationName
    )

    $lastException = $null

    for ($attempt = 0; $attempt -le $MaxRetryCount; $attempt++) {
        try {
            $ClientContext.ExecuteQuery()
            return
        }
        catch {
            $lastException = $_.Exception

            $message = ""
            if ($null -ne $lastException -and $null -ne $lastException.Message) {
                $message = [string]$lastException.Message
            }

            $isThrottled = ($message -like "*429*" -or $message -like "*Too Many Requests*" -or $message -like "*503*" -or $message -like "*throttl*")

            if (-not $isThrottled) {
                throw
            }

            if ($attempt -ge $MaxRetryCount) {
                throw "SharePoint throttling persisted after $($MaxRetryCount + 1) attempt(s) for operation '$OperationName'. Last error: $message"
            }

            $waitSecondsDouble = [Math]::Pow(2, $attempt) * $RetryBaseDelaySeconds
            $waitSeconds = [int][Math]::Ceiling([Math]::Min(60, $waitSecondsDouble))
            Write-Warning "SharePoint throttling during '$OperationName' (attempt $($attempt + 1)/$($MaxRetryCount + 1)). Waiting $waitSeconds second(s) before retry."
            Start-Sleep -Seconds $waitSeconds
        }
    }

    if ($null -ne $lastException) {
        throw $lastException
    }

    throw "ExecuteQuery retry loop ended unexpectedly for operation '$OperationName'."
}

# ==========================================
# Function: Validate CSV Row
# ==========================================
function Test-CsvRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [PSCustomObject]$Row
    )

    $errors = @()

    if ([string]::IsNullOrWhiteSpace($Row.SiteUrl)) {
        $errors += "SiteUrl is empty"
    }

    if ([string]::IsNullOrWhiteSpace($Row.LibraryName)) {
        $errors += "LibraryName is empty"
    }

    if ($errors.Count -gt 0) {
        return @{
            IsValid = $false
            Errors = ($errors -join "; ")
        }
    }

    return @{
        IsValid = $true
        Errors = ""
    }
}

# ==========================================
# Function: Get Direct Child Item Count
# ==========================================
function Get-DirectChildItemCount {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$ListName,

        [Parameter(Mandatory = $true)]
        [string]$FolderServerRelativeUrl
    )

    $connection = $null
    $clientContext = $null
    $clientList = $null
    $listItemPosition = $null
    $currentPageItems = $null
    $itemCount = 0
    $escapedFolderPath = ""
    $directChildrenCaml = ""

    $escapedFolderPath = [System.Security.SecurityElement]::Escape($FolderServerRelativeUrl)

    $directChildrenCaml = @"
<View Scope='RecursiveAll'>
    <ViewFields>
        <FieldRef Name='ID' />
        <FieldRef Name='FileLeafRef' />
        <FieldRef Name='FSObjType' />
    </ViewFields>
    <Query>
        <Where>
            <Eq>
                <FieldRef Name='FileDirRef' />
                <Value Type='Lookup'>$escapedFolderPath</Value>
            </Eq>
        </Where>
        <OrderBy Override='TRUE'>
            <FieldRef Name='ID' Ascending='TRUE' />
        </OrderBy>
    </Query>
    <RowLimit Paged='TRUE'>$($QueryPageSize)</RowLimit>
</View>
"@

    $connection = Get-PnPConnection -ErrorAction Stop
    $clientContext = $connection.Context
    $clientList = $clientContext.Web.Lists.GetByTitle($ListName)

    do {
        $camlQuery = New-Object Microsoft.SharePoint.Client.CamlQuery
        $camlQuery.ViewXml = $directChildrenCaml
        $camlQuery.ListItemCollectionPosition = $listItemPosition

        $currentPageItems = $clientList.GetItems($camlQuery)
        $clientContext.Load($currentPageItems)
        Invoke-CSOMExecuteQueryWithRetry -ClientContext $clientContext -OperationName "Direct child count: $ListName"

        foreach ($item in $currentPageItems) {
            $leafName = [string]$item["FileLeafRef"]
            $fsObjType = 0
            if ($null -ne $item["FSObjType"] -and $item["FSObjType"].ToString() -ne "") {
                $fsObjType = [int]$item["FSObjType"]
            }

            if ($leafName -eq "Forms" -and $fsObjType -eq 1) {
                continue
            }

            $itemCount++
        }

        $listItemPosition = $currentPageItems.ListItemCollectionPosition
    }
    while ($null -ne $listItemPosition)

    return $itemCount
}

# ==========================================
# Function: Get Library Folder URLs
# ==========================================
function Get-LibraryFolderUrls {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$LibraryName,

        [string]$FolderName = ""
    )

    $list = $null
    $rootServerRelativeUrl = ""
    $folderRows = New-Object System.Collections.ArrayList
    $web = $null
    $rootSiteRelativeUrl = ""
    $rootItemCount = 0
    $connection = $null
    $clientContext = $null
    $clientList = $null
    $listItemPosition = $null
    $currentPageItems = $null
    $scannedItemCount = 0
    $scopeRootServerRelativeUrl = ""
    $scopeRootSiteRelativeUrl = ""
    $matchingRootFolderItem = $null
    $rootFolderLookupCaml = ""
    $escapedRootPath = ""
    $escapedFolderName = ""

    try {
        $list = Get-PnPList -Identity $LibraryName -Includes RootFolder -ErrorAction Stop
    }
    catch {
        throw "Library '$LibraryName' not found at '$SiteUrl'. $_"
    }

    if ($null -eq $list -or $null -eq $list.RootFolder) {
        throw "Unable to retrieve root folder for library '$LibraryName'."
    }

    $web = Get-PnPWeb -Includes ServerRelativeUrl -ErrorAction Stop
    $rootServerRelativeUrl = [string]$list.RootFolder.ServerRelativeUrl

    $webServerRelativeUrl = [string]$web.ServerRelativeUrl
    if ($webServerRelativeUrl -eq "/") {
        $rootSiteRelativeUrl = $rootServerRelativeUrl.TrimStart('/')
    }
    elseif ($rootServerRelativeUrl.StartsWith($webServerRelativeUrl + "/", [System.StringComparison]::OrdinalIgnoreCase)) {
        $rootSiteRelativeUrl = $rootServerRelativeUrl.Substring($webServerRelativeUrl.Length + 1)
    }
    elseif ($rootServerRelativeUrl.Equals($webServerRelativeUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
        $rootSiteRelativeUrl = ""
    }
    else {
        $rootSiteRelativeUrl = $rootServerRelativeUrl.TrimStart('/')
    }

    $scopeRootServerRelativeUrl = $rootServerRelativeUrl
    $scopeRootSiteRelativeUrl = $rootSiteRelativeUrl

    if (-not [string]::IsNullOrWhiteSpace($FolderName)) {
        $escapedRootPath = [System.Security.SecurityElement]::Escape($rootServerRelativeUrl)
        $escapedFolderName = [System.Security.SecurityElement]::Escape($FolderName.Trim())

        $rootFolderLookupCaml = @"
<View Scope='RecursiveAll'>
    <ViewFields>
        <FieldRef Name='ID' />
        <FieldRef Name='FileRef' />
        <FieldRef Name='FileLeafRef' />
    </ViewFields>
    <Query>
        <Where>
            <And>
                <Eq>
                    <FieldRef Name='FSObjType' />
                    <Value Type='Integer'>1</Value>
                </Eq>
                <And>
                    <Eq>
                        <FieldRef Name='FileDirRef' />
                        <Value Type='Lookup'>$escapedRootPath</Value>
                    </Eq>
                    <Eq>
                        <FieldRef Name='FileLeafRef' />
                        <Value Type='Text'>$escapedFolderName</Value>
                    </Eq>
                </And>
            </And>
        </Where>
        <OrderBy Override='TRUE'>
            <FieldRef Name='ID' Ascending='TRUE' />
        </OrderBy>
    </Query>
    <RowLimit Paged='TRUE'>1</RowLimit>
</View>
"@

        $matchingRootFolderItem = @(Get-PnPListItem -List $LibraryName -Query $rootFolderLookupCaml -PageSize 1 -ErrorAction Stop) | Select-Object -First 1

        if ($null -eq $matchingRootFolderItem) {
            throw "Root-level folder '$FolderName' was not found in library '$LibraryName'."
        }

        $scopeRootServerRelativeUrl = [string]$matchingRootFolderItem["FileRef"]

        if ([string]::IsNullOrWhiteSpace($scopeRootServerRelativeUrl)) {
            throw "Unable to resolve server relative URL for root folder '$FolderName' in library '$LibraryName'."
        }

        if ($webServerRelativeUrl -eq "/") {
            $scopeRootSiteRelativeUrl = $scopeRootServerRelativeUrl.TrimStart('/')
        }
        elseif ($scopeRootServerRelativeUrl.StartsWith($webServerRelativeUrl + "/", [System.StringComparison]::OrdinalIgnoreCase)) {
            $scopeRootSiteRelativeUrl = $scopeRootServerRelativeUrl.Substring($webServerRelativeUrl.Length + 1)
        }
        elseif ($scopeRootServerRelativeUrl.Equals($webServerRelativeUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
            $scopeRootSiteRelativeUrl = ""
        }
        else {
            $scopeRootSiteRelativeUrl = $scopeRootServerRelativeUrl.TrimStart('/')
        }
    }

    if ([string]::IsNullOrWhiteSpace($FolderName)) {
        Write-Host "Querying all folders from library root: $scopeRootServerRelativeUrl"
    }
    else {
        Write-Host "Querying scoped root folder '$FolderName': $scopeRootServerRelativeUrl"
    }

    $rootItemCount = Get-DirectChildItemCount -ListName $LibraryName -FolderServerRelativeUrl $scopeRootServerRelativeUrl
    [void]$folderRows.Add([PSCustomObject]@{
        TargetFolderUrl = $SiteUrl.TrimEnd('/') + $scopeRootServerRelativeUrl
        ItemsCount = [int]$rootItemCount
    })

    $pagedScanCaml = @"
<View Scope='RecursiveAll'>
    <ViewFields>
        <FieldRef Name='ID' />
        <FieldRef Name='FileRef' />
        <FieldRef Name='FileLeafRef' />
        <FieldRef Name='FSObjType' />
        <FieldRef Name='ItemChildCount' />
        <FieldRef Name='FolderChildCount' />
    </ViewFields>
    <Query>
        <OrderBy Override='TRUE'>
            <FieldRef Name='ID' Ascending='TRUE' />
        </OrderBy>
    </Query>
    <RowLimit Paged='TRUE'>$($QueryPageSize)</RowLimit>
</View>
"@

    $connection = Get-PnPConnection -ErrorAction Stop
    $clientContext = $connection.Context
    $clientList = $clientContext.Web.Lists.GetByTitle($LibraryName)

    do {
        $camlQuery = New-Object Microsoft.SharePoint.Client.CamlQuery
        $camlQuery.ViewXml = $pagedScanCaml
        $camlQuery.ListItemCollectionPosition = $listItemPosition

        $currentPageItems = $clientList.GetItems($camlQuery)
        $clientContext.Load($currentPageItems)
        Invoke-CSOMExecuteQueryWithRetry -ClientContext $clientContext -OperationName "Paged folder scan: $LibraryName"

        foreach ($item in $currentPageItems) {
            $scannedItemCount++

            $fileRef = [string]$item["FileRef"]
            if ([string]::IsNullOrWhiteSpace($fileRef)) {
                continue
            }

            $fsObjType = 0
            if ($null -ne $item["FSObjType"] -and $item["FSObjType"].ToString() -ne "") {
                $fsObjType = [int]$item["FSObjType"]
            }

            if ($fsObjType -ne 1) {
                continue
            }

            if (
                -not $fileRef.Equals($scopeRootServerRelativeUrl, [System.StringComparison]::OrdinalIgnoreCase) -and
                -not $fileRef.StartsWith($scopeRootServerRelativeUrl + "/", [System.StringComparison]::OrdinalIgnoreCase)
            ) {
                continue
            }

            $leafName = [string]$item["FileLeafRef"]
            if ($leafName -eq "Forms") {
                continue
            }

            if ($fileRef.Equals($scopeRootServerRelativeUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
                continue
            }

            $itemChildCount = 0
            if ($null -ne $item["ItemChildCount"] -and $item["ItemChildCount"].ToString() -ne "") {
                $itemChildCount = [int]$item["ItemChildCount"]
            }

            $folderChildCount = 0
            if ($null -ne $item["FolderChildCount"] -and $item["FolderChildCount"].ToString() -ne "") {
                $folderChildCount = [int]$item["FolderChildCount"]
            }

            [void]$folderRows.Add([PSCustomObject]@{
                TargetFolderUrl = $SiteUrl.TrimEnd('/') + $fileRef
                ItemsCount = ($itemChildCount + $folderChildCount)
            })
        }

        $listItemPosition = $currentPageItems.ListItemCollectionPosition
    }
    while ($null -ne $listItemPosition)

    Write-Verbose "Scanned $scannedItemCount list item(s) in '$LibraryName' using pagination."

    return @($folderRows | Sort-Object TargetFolderUrl -Unique)
}

# ==========================================
# Function: Process Single Row
# ==========================================
function Invoke-GenerateFolderPairsForRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$RowIndex,

        [Parameter(Mandatory = $true)]
        [string]$SiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$LibraryName

        ,
        [string]$FolderName = ""
    )

    $rowResult = $null
    $folderRows = @()
    $folderCount = 0

    try {
        Get-PnPConnectionForSite -SiteUrl $SiteUrl
        $folderRows = @(Get-LibraryFolderUrls -SiteUrl $SiteUrl -LibraryName $LibraryName -FolderName $FolderName)
        $folderCount = $folderRows.Count

        foreach ($folderRow in $folderRows) {
            $script:FolderPairs += [PSCustomObject]@{
                TargetFolderUrl = $folderRow.TargetFolderUrl
                ItemsCount = [int]$folderRow.ItemsCount
                SiteUrl = $SiteUrl
                LibraryName = $LibraryName
                FolderName = $FolderName
                Timestamp = (Get-Date).ToString("s")
            }
        }

        $script:TotalFoldersDiscovered += $folderCount

        $rowResult = [PSCustomObject]@{
            RowNumber = $RowIndex
            SiteUrl = $SiteUrl
            LibraryName = $LibraryName
            FolderName = $FolderName
            Status = "Completed"
            IsSuccessful = $true
            FolderCount = $folderCount
            Message = "Discovered $folderCount folder(s), including nested folders."
            Timestamp = (Get-Date).ToString("s")
        }
    }
    catch {
        $rowResult = [PSCustomObject]@{
            RowNumber = $RowIndex
            SiteUrl = $SiteUrl
            LibraryName = $LibraryName
            FolderName = $FolderName
            Status = "Failed"
            IsSuccessful = $false
            FolderCount = 0
            Message = $_.Exception.Message
            Timestamp = (Get-Date).ToString("s")
        }
    }
    finally {
        Disconnect-PnPIfConnected
    }

    return $rowResult
}

# ==========================================
# Function: Main Processing Function
# ==========================================
function Invoke-MainFunction {
    [CmdletBinding()]
    param()

    Write-Host "Reading CSV file: $CsvInputPath"
    $rows = Import-Csv -Path $CsvInputPath -Encoding UTF8

    Write-Host "Validating CSV format..."
    Test-CsvFormat -Rows $rows
    $script:TotalRows = $rows.Count

    Write-Host "Processing $($rows.Count) row(s)..."

    for ($rowIndex = 0; $rowIndex -lt $rows.Count; $rowIndex++) {
        $row = $rows[$rowIndex]
        $displayRowNumber = $rowIndex + 1

        $validation = Test-CsvRow -Row $row
        if (-not $validation.IsValid) {
            $script:Results += [PSCustomObject]@{
                RowNumber = $displayRowNumber
                SiteUrl = $row.SiteUrl
                LibraryName = $row.LibraryName
                FolderName = $row.FolderName
                Status = "Failed"
                IsSuccessful = $false
                FolderCount = 0
                Message = $validation.Errors
                Timestamp = (Get-Date).ToString("s")
            }

            $script:FailedRows++
            $script:ProcessedRows++
            Write-Warning "Row $displayRowNumber validation failed: $($validation.Errors)"
            continue
        }

        Write-Host "Processing row $displayRowNumber of $($rows.Count)..."

        $result = Invoke-GenerateFolderPairsForRow -RowIndex $displayRowNumber -SiteUrl ([string]$row.SiteUrl).Trim() -LibraryName ([string]$row.LibraryName).Trim() -FolderName ([string]$row.FolderName).Trim()
        $script:Results += $result

        if ($result.IsSuccessful) {
            $script:SuccessfulRows++
        }
        else {
            $script:FailedRows++
        }

        $script:ProcessedRows++
        Start-Sleep -Milliseconds 250
    }
}

# ==========================================
# Main Execution
# ==========================================
$startTime = Get-Date

try {
    Write-Host "Starting Generate FolderPairs CSV script (SPO-only)..." -ForegroundColor Cyan
    Initialize-Credential
    Invoke-MainFunction

    $endTime = Get-Date
    $duration = $endTime - $startTime

    $folderPairsFile = Join-Path $OutputFolder "FolderPairs_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $resultFile = Join-Path $OutputFolder "Results_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"

    Write-Host "Exporting generated folder pairs to CSV..."
    $script:FolderPairs | Select-Object TargetFolderUrl, ItemsCount | Export-Csv -Path $folderPairsFile -NoTypeInformation -Encoding UTF8 -Force

    Write-Host "Exporting row results to CSV..."
    $script:Results | Export-Csv -Path $resultFile -NoTypeInformation -Encoding UTF8 -Force

    $summary = @{
        ExecutionTime = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalRows = $script:TotalRows
        ProcessedRows = $script:ProcessedRows
        SuccessfulRows = $script:SuccessfulRows
        FailedRows = $script:FailedRows
        TotalFoldersDiscovered = $script:TotalFoldersDiscovered
        FolderPairsFile = $folderPairsFile
        ResultFile = $resultFile
        TimestampStart = $startTime.ToString("s")
        TimestampEnd = $endTime.ToString("s")
    }

    Write-Host "Exporting summary to JSON..."
    $summary | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Generate FolderPairs CSV Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total Rows:               $($summary.TotalRows)"
    Write-Host "Processed Rows:           $($summary.ProcessedRows)"
    Write-Host "Successful Rows:          $($summary.SuccessfulRows)" -ForegroundColor Green
    Write-Host "Failed Rows:              $($summary.FailedRows)" -ForegroundColor $(if ($summary.FailedRows -gt 0) { "Red" } else { "Green" })
    Write-Host "Total Folders Discovered: $($summary.TotalFoldersDiscovered)" -ForegroundColor Green
    Write-Host "FolderPairs CSV:          $folderPairsFile"
    Write-Host "Results CSV:              $resultFile"
    Write-Host "Summary JSON:             $summaryFile"
    Write-Host "Execution Time:           $($summary.ExecutionTime)"
    Write-Host "==========================================" -ForegroundColor Cyan
}
catch {
    Write-Error "Fatal error: $_"
    exit 1
}
finally {
    Disconnect-PnPIfConnected
    Write-Host "Script execution completed." -ForegroundColor Cyan
}
#\.Generate-FolderPairs-CSV.ps1 -CsvInputPath ".\LibraryFolders-Template.csv" -ClientId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" -TargetUsername "admin@tenant.onmicrosoft.com" -TargetPassword "Password123!" -MaxRetryCount 12 -RetryBaseDelaySeconds 4 -QueryPageSize 500

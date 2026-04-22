#Requires -Version 5.1
<#
.SYNOPSIS
    Removes (recycles) SharePoint Online list items based on modified date criteria.

.DESCRIPTION
    Reads a CSV file containing SharePoint Online site URLs, list names, date thresholds,
    and date conditions (Before or After). For each row, connects to the specified SPO site
    and recycles all list items matching the date criteria to the recycle bin.
    
    The script supports bulk item deletion with before/after date filtering.
    Results are logged to CSV and summary is output as JSON.

.PARAMETER CsvInputPath
    Path to CSV file with required columns: SiteUrl, ListName, DateThreshold, DateCondition.
    DateCondition must be "Before" or "After".

.PARAMETER ClientId
    Azure AD app client ID for SPO PnP authentication.

.PARAMETER TargetUsername
    SPO username for authentication (hardcoded credential).

.PARAMETER TargetPassword
    SPO password for authentication (hardcoded credential).

.PARAMETER OutputFolder
    Folder path for output files (Results CSV and Summary JSON).
    Default: ./RemoveListItemsLog-{timestamp}

.EXAMPLE
    .\Remove-ListItemsByModifiedDate.ps1 `
        -CsvInputPath ".\ListItems-Template.csv" `
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

    [string]$OutputFolder = "./RemoveListItemsLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
)

# ==========================================
# Strict Mode and Error Preference
# ==========================================
Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

# ==========================================
# Global Variables & Initialization
# ==========================================
$script:Results         = @()
$script:ProcessedRows   = 0
$script:SuccessfulRows  = 0
$script:FailedRows      = 0
$script:Credential      = $null
$script:ItemsRecycled   = 0
$script:TotalRows       = 0

if (-not (Test-Path $CsvInputPath)) {
    throw "CSV input file not found: $CsvInputPath"
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
# Function: Disconnect from SPO Site
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
    $requiredColumns = @("SiteUrl", "ListName", "DateThreshold", "DateCondition")

    foreach ($column in $requiredColumns) {
        if (-not ($first.PSObject.Properties.Name -contains $column)) {
            throw "CSV must contain required column: $column"
        }
    }
}

# ==========================================
# Function: Validate CSV Row
# ==========================================
function Test-CsvRow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [PSCustomObject]$Row,

        [Parameter(Mandatory = $true)]
        [int]$RowIndex
    )

    $errors = @()

    if ([string]::IsNullOrWhiteSpace($Row.SiteUrl)) {
        $errors += "SiteUrl is empty"
    }

    if ([string]::IsNullOrWhiteSpace($Row.ListName)) {
        $errors += "ListName is empty"
    }

    if ([string]::IsNullOrWhiteSpace($Row.DateThreshold)) {
        $errors += "DateThreshold is empty"
    }
    else {
        $parsedDate = [datetime]::MinValue
        if (-not [datetime]::TryParse($Row.DateThreshold, [ref]$parsedDate)) {
            $errors += "DateThreshold is not a valid date: $($Row.DateThreshold)"
        }
    }

    if ([string]::IsNullOrWhiteSpace($Row.DateCondition)) {
        $errors += "DateCondition is empty"
    }
    else {
        $condition = $Row.DateCondition.Trim()
        if ($condition -ne "Before" -and $condition -ne "After") {
            $errors += "DateCondition must be 'Before' or 'After', got: $condition"
        }
    }

    if ($errors.Count -gt 0) {
        return @{
            IsValid = $false
            Errors  = $errors -join "; "
        }
    }

    return @{
        IsValid = $true
        Errors  = ""
    }
}

# ==========================================
# Function: Build CAML Query for Date Filtering
# ==========================================
function Build-DateFilterCAML {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [datetime]$DateThreshold,

        [Parameter(Mandatory = $true)]
        [string]$DateCondition
    )

    $dateString = $DateThreshold.ToString("yyyy-MM-ddTHH:mm:ssZ")
    
    if ($DateCondition -eq "Before") {
        $operator = "Lt"
    }
    else {
        $operator = "Gt"
    }

    $camlQuery = @"
<View>
  <Query>
    <Where>
      <$operator>
        <FieldRef Name="Modified" />
        <Value Type="DateTime">$dateString</Value>
      </$operator>
    </Where>
  </Query>
</View>
"@

    return $camlQuery
}

# ==========================================
# Function: Recycle List Items by Date
# ==========================================
function Invoke-RecycleListItemsByDate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$SiteUrl,

        [Parameter(Mandatory = $true)]
        [string]$ListName,

        [Parameter(Mandatory = $true)]
        [datetime]$DateThreshold,

        [Parameter(Mandatory = $true)]
        [string]$DateCondition,

        [Parameter(Mandatory = $true)]
        [int]$RowIndex
    )

    $rowResult = $null
    $itemsRecycledCount = 0

    try {
        Get-PnPConnectionForSite -SiteUrl $SiteUrl

        $list = Get-PnPList -Identity $ListName -ErrorAction Stop
        if ($null -eq $list) {
            throw "List '$ListName' not found"
        }

        Write-Verbose "Building CAML query for date filtering..."
        $camlQuery = Build-DateFilterCAML -DateThreshold $DateThreshold -DateCondition $DateCondition

        Write-Verbose "Retrieving filtered items from list: $ListName"
        $items = Get-PnPListItem -List $ListName -Query $camlQuery -PageSize 5000 -ErrorAction Stop

        if ($null -eq $items -or $items.Count -eq 0) {
            $rowResult = [PSCustomObject]@{
                RowNumber            = $RowIndex
                SiteUrl              = $SiteUrl
                ListName             = $ListName
                DateThreshold        = $DateThreshold.ToString("s")
                DateCondition        = $DateCondition
                Status               = "Completed"
                IsSuccessful         = $true
                ItemsRecycled        = 0
                Message              = "No items matched the date criteria"
                Timestamp            = (Get-Date).ToString("s")
            }
            return $rowResult
        }

        $itemCount = if ($items -is [array]) { $items.Count } else { 1 }
        Write-Host "Found $itemCount items to recycle from '$ListName'"

        foreach ($item in $items) {
            try {
                Remove-PnPListItem -List $ListName -Identity $item.Id -Force -ErrorAction Stop
                $itemsRecycledCount++
            }
            catch {
                Write-Warning "Failed to recycle item $($item.Id) from '$ListName': $_"
            }
        }

        $rowResult = [PSCustomObject]@{
            RowNumber            = $RowIndex
            SiteUrl              = $SiteUrl
            ListName             = $ListName
            DateThreshold        = $DateThreshold.ToString("s")
            DateCondition        = $DateCondition
            Status               = "Completed"
            IsSuccessful         = $true
            ItemsRecycled        = $itemsRecycledCount
            Message              = "Successfully recycled $itemsRecycledCount item(s)"
            Timestamp            = (Get-Date).ToString("s")
        }

        $script:ItemsRecycled += $itemsRecycledCount
    }
    catch {
        $rowResult = [PSCustomObject]@{
            RowNumber            = $RowIndex
            SiteUrl              = $SiteUrl
            ListName             = $ListName
            DateThreshold        = $DateThreshold.ToString("s")
            DateCondition        = $DateCondition
            Status               = "Failed"
            IsSuccessful         = $false
            ItemsRecycled        = 0
            Message              = $_.Exception.Message
            Timestamp            = (Get-Date).ToString("s")
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

        $validation = Test-CsvRow -Row $row -RowIndex $displayRowNumber

        if (-not $validation.IsValid) {
            $failedResult = [PSCustomObject]@{
                RowNumber            = $displayRowNumber
                SiteUrl              = $row.SiteUrl
                ListName             = $row.ListName
                DateThreshold        = $row.DateThreshold
                DateCondition        = $row.DateCondition
                Status               = "Skipped"
                IsSuccessful         = $false
                ItemsRecycled        = 0
                Message              = $validation.Errors
                Timestamp            = (Get-Date).ToString("s")
            }
            $script:Results += $failedResult
            $script:FailedRows++
            $script:ProcessedRows++
            Write-Warning "Row $displayRowNumber validation failed: $($validation.Errors)"
            continue
        }

        Write-Host "Processing row $displayRowNumber of $($rows.Count)..."

        $dateThreshold = [datetime]::Parse($row.DateThreshold)
        $dateCondition = $row.DateCondition.Trim()

        $result = Invoke-RecycleListItemsByDate `
            -SiteUrl $row.SiteUrl `
            -ListName $row.ListName `
            -DateThreshold $dateThreshold `
            -DateCondition $dateCondition `
            -RowIndex $displayRowNumber

        $script:Results += $result

        if ($result.IsSuccessful) {
            $script:SuccessfulRows++
        }
        else {
            $script:FailedRows++
        }

        $script:ProcessedRows++

        Start-Sleep -Milliseconds 500
    }
}

# ==========================================
# Main Execution
# ==========================================
$startTime = Get-Date

try {
    Write-Host "Starting Remove List Items by Modified Date script..." -ForegroundColor Cyan
    Initialize-Credential
    Invoke-MainFunction

    $endTime = Get-Date
    $duration = $endTime - $startTime

    $resultFile  = Join-Path $OutputFolder "Results_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"

    Write-Host "Exporting results to CSV..."
    $script:Results | Export-Csv -Path $resultFile -NoTypeInformation -Encoding UTF8 -Force

    $summary = @{
        ExecutionTime      = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalRows          = $script:TotalRows
        ProcessedRows      = $script:ProcessedRows
        SuccessfulRows     = $script:SuccessfulRows
        FailedRows         = $script:FailedRows
        TotalItemsRecycled = $script:ItemsRecycled
        ResultFile         = $resultFile
        TimestampStart     = $startTime.ToString("s")
        TimestampEnd       = $endTime.ToString("s")
    }

    Write-Host "Exporting summary to JSON..."
    $summary | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Remove List Items by Modified Date Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total Rows:           $($summary.TotalRows)"
    Write-Host "Processed Rows:       $($summary.ProcessedRows)"
    Write-Host "Successful Rows:      $($summary.SuccessfulRows)" -ForegroundColor Green
    Write-Host "Failed Rows:          $($summary.FailedRows)" -ForegroundColor $(if ($summary.FailedRows -gt 0) { "Red" } else { "Green" })
    Write-Host "Total Items Recycled: $($summary.TotalItemsRecycled)" -ForegroundColor Green
    Write-Host "Result CSV:           $resultFile"
    Write-Host "Summary JSON:         $summaryFile"
    Write-Host "Execution Time:       $($summary.ExecutionTime)"
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

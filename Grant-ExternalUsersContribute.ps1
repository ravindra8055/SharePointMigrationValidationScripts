#Requires -Version 5.1
<#
.SYNOPSIS
    Grants Contribute permission to SharePoint groups that match a name prefix.

.DESCRIPTION
    Connects to a SharePoint Online site collection and finds all site groups whose
    names start with a configured prefix (default: Dalmore External Users).
    For each matching group, the script ensures the group has site-level permission
    for the configured role (default: Contribute).

    The script writes:
    1) Results_<timestamp>.csv with per-group processing status
    2) Summary_<timestamp>.json with execution totals

.PARAMETER TargetSiteUrl
    SharePoint Online site collection URL.

.PARAMETER ClientId
    Azure AD app client ID for PnP authentication.

.PARAMETER TargetUsername
    SharePoint Online username used for authentication.

.PARAMETER TargetPassword
    SharePoint Online password used for authentication.

.PARAMETER GroupNamePrefix
    Group name prefix filter.
    Default: Dalmore External Users

.PARAMETER PermissionLevel
    Site permission level to ensure for each matching group.
    Default: Contribute

.PARAMETER OutputFolder
    Folder path for output files.
    Default: ./GrantDalmoreExternalUsersContributeLog-{timestamp}

.EXAMPLE
    .\Grant-DalmoreExternalUsersContribute.ps1 `
        -TargetSiteUrl "https://tenant.sharepoint.com/sites/Finance" `
        -ClientId "11111111-2222-3333-4444-555555555555" `
        -TargetUsername "admin@tenant.onmicrosoft.com" `
        -TargetPassword "Password123!" `
        -GroupNamePrefix "Dalmore External Users" `
        -PermissionLevel "Contribute"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$TargetSiteUrl,

    [Parameter(Mandatory = $true)]
    [string]$ClientId,

    [Parameter(Mandatory = $true)]
    [string]$TargetUsername,

    [Parameter(Mandatory = $true)]
    [string]$TargetPassword,

    [Parameter(Mandatory = $false)]
    [string]$GroupNamePrefix = "Dalmore External Users",

    [Parameter(Mandatory = $false)]
    [string]$PermissionLevel = "Contribute",

    [string]$OutputFolder = "./GrantDalmoreExternalUsersContributeLog-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
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
$script:Credential = $null

if ([string]::IsNullOrWhiteSpace($TargetSiteUrl)) {
    throw "TargetSiteUrl cannot be empty."
}

if ([string]::IsNullOrWhiteSpace($ClientId)) {
    throw "ClientId cannot be empty."
}

if ([string]::IsNullOrWhiteSpace($TargetUsername)) {
    throw "TargetUsername cannot be empty."
}

if ([string]::IsNullOrWhiteSpace($TargetPassword)) {
    throw "TargetPassword cannot be empty."
}

if ([string]::IsNullOrWhiteSpace($GroupNamePrefix)) {
    throw "GroupNamePrefix cannot be empty."
}

if ([string]::IsNullOrWhiteSpace($PermissionLevel)) {
    throw "PermissionLevel cannot be empty."
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
function Test-InputParameters {
    [CmdletBinding()]
    param()

    if (-not ($TargetSiteUrl -match '^https://')) {
        throw "TargetSiteUrl must be an HTTPS URL."
    }
}

# ==========================================
# Function: Core domain functions
# ==========================================
function Get-MatchingGroups {
    [CmdletBinding()]
    param()

    $allGroups = Get-PnPGroup -ErrorAction Stop
    $matchingGroups = @()

    foreach ($group in $allGroups) {
        if ($group.Title -like "$GroupNamePrefix*") {
            $matchingGroups += $group
        }
    }

    return $matchingGroups
}

function Test-GroupHasRole {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Group,

        [Parameter(Mandatory = $true)]
        [string]$RoleName
    )

    $ctx = Get-PnPContext
    $web = Get-PnPWeb -ErrorAction Stop

    $ctx.Load($web.RoleAssignments)
    $ctx.ExecuteQuery()

    foreach ($roleAssignment in $web.RoleAssignments) {
        $ctx.Load($roleAssignment.Member)
        $ctx.Load($roleAssignment.RoleDefinitionBindings)
        $ctx.ExecuteQuery()

        if ($roleAssignment.Member.LoginName -eq $Group.LoginName) {
            foreach ($roleDef in $roleAssignment.RoleDefinitionBindings) {
                if ($roleDef.Name -eq $RoleName) {
                    return $true
                }
            }

            return $false
        }
    }

    return $false
}

function Set-GroupSitePermission {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Group,

        [Parameter(Mandatory = $true)]
        [int]$RowNumber
    )

    try {
        $hasRole = Test-GroupHasRole -Group $Group -RoleName $PermissionLevel

        if ($hasRole) {
            return [PSCustomObject]@{
                RowNumber = $RowNumber
                SiteUrl = $TargetSiteUrl
                GroupName = $Group.Title
                Status = "AlreadyExists"
                IsSuccessful = $true
                Message = "Group already has '$PermissionLevel' permission."
                Timestamp = (Get-Date).ToString("s")
            }
        }

        Set-PnPGroupPermissions -Identity $Group.Title -AddRole $PermissionLevel -ErrorAction Stop

        return [PSCustomObject]@{
            RowNumber = $RowNumber
            SiteUrl = $TargetSiteUrl
            GroupName = $Group.Title
            Status = "Added"
            IsSuccessful = $true
            Message = "Granted '$PermissionLevel' permission to group."
            Timestamp = (Get-Date).ToString("s")
        }
    }
    catch {
        return [PSCustomObject]@{
            RowNumber = $RowNumber
            SiteUrl = $TargetSiteUrl
            GroupName = $Group.Title
            Status = "Failed"
            IsSuccessful = $false
            Message = "Failed to assign permission: $($_.Exception.Message)"
            Timestamp = (Get-Date).ToString("s")
        }
    }
}

# ==========================================
# Function: Main processing function
# ==========================================
function Invoke-GrantDalmoreExternalUsersContribute {
    [CmdletBinding()]
    param()

    $startTime = Get-Date
    $rowIndex = 0

    Get-PnPConnectionForSite -SiteUrl $TargetSiteUrl

    $matchingGroups = Get-MatchingGroups

    if ($matchingGroups.Count -eq 0) {
        $script:ProcessedRows = 1
        $script:SuccessfulRows = 1

        $script:Results += [PSCustomObject]@{
            RowNumber = 1
            SiteUrl = $TargetSiteUrl
            GroupName = ""
            Status = "Skipped"
            IsSuccessful = $true
            Message = "No groups found with prefix '$GroupNamePrefix'."
            Timestamp = (Get-Date).ToString("s")
        }
    }
    else {
        foreach ($group in $matchingGroups) {
            $rowIndex++
            Write-Host "Processing group $rowIndex/$($matchingGroups.Count): $($group.Title)"

            $result = Set-GroupSitePermission -Group $group -RowNumber $rowIndex
            $script:Results += $result

            $script:ProcessedRows++

            if ($result.IsSuccessful) {
                $script:SuccessfulRows++
            }
            else {
                $script:FailedRows++
            }
        }
    }

    $endTime = Get-Date
    $duration = $endTime - $startTime

    $resultFile = Join-Path $OutputFolder "Results_$(Get-Date -Format 'yyyyMMdd-HHmmss').csv"
    $summaryFile = Join-Path $OutputFolder "Summary_$(Get-Date -Format 'yyyyMMdd-HHmmss').json"

    $script:Results | Export-Csv -Path $resultFile -NoTypeInformation -Encoding UTF8 -Force

    $summary = @{
        ExecutionTime = "$($duration.Hours)h $($duration.Minutes)m $($duration.Seconds)s"
        TotalRows = $matchingGroups.Count
        ProcessedRows = $script:ProcessedRows
        SuccessfulRows = $script:SuccessfulRows
        FailedRows = $script:FailedRows
        ResultFile = $resultFile
        TimestampStart = $startTime.ToString("s")
        TimestampEnd = $endTime.ToString("s")
    }

    $summary | ConvertTo-Json | Set-Content -Path $summaryFile -Encoding UTF8

    $failedColor = "Green"
    if ($summary.FailedRows -gt 0) {
        $failedColor = "Red"
    }

    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Grant Dalmore External Users Contribute Summary" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "Total Rows:       $($summary.TotalRows)"
    Write-Host "Processed Rows:   $($summary.ProcessedRows)"
    Write-Host "Successful Rows:  $($summary.SuccessfulRows)" -ForegroundColor Green
    Write-Host "Failed Rows:      $($summary.FailedRows)" -ForegroundColor $failedColor
    Write-Host "Result CSV:       $resultFile"
    Write-Host "Summary JSON:     $summaryFile"
    Write-Host "Execution Time:   $($summary.ExecutionTime)"
    Write-Host "==========================================" -ForegroundColor Cyan
}

# ==========================================
# Main Execution
# ==========================================
try {
    Write-Host "Starting permission assignment for matching SharePoint groups..." -ForegroundColor Cyan
    Initialize-Credential
    Test-InputParameters
    Invoke-GrantDalmoreExternalUsersContribute
}
catch {
    Write-Error "Fatal error: $_"
    exit 1
}
finally {
    Disconnect-PnPOnline -ErrorAction SilentlyContinue
    Write-Host "Script execution completed." -ForegroundColor Cyan
}

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SourceSiteUrl,

    [Parameter(Mandatory = $true)]
    [string]$TargetSiteUrl,

    [string]$OutputFolder = "."
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -----------------------------
# Hardcoded target credentials
# -----------------------------
# NOTE: For production, move these values to a secure vault.
$TargetUserName = "hardcoded.user@tenant.onmicrosoft.com"
$TargetPasswordPlain = "HardcodedPasswordHere"
$TargetClientId = "00000000-0000-0000-0000-000000000000"

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
            Add-Type -Path $runtimeDll
            Add-Type -Path $clientDll
            $loaded = $true
            break
        }
    }

    if (-not $loaded) {
        throw "Unable to load CSOM assemblies. Install SharePoint CSOM or run on a SharePoint server with ISAPI DLLs."
    }
}

function Get-SourceContext {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    $ctx = New-Object Microsoft.SharePoint.Client.ClientContext($Url)
    $ctx.Credentials = [System.Net.CredentialCache]::DefaultNetworkCredentials
    return $ctx
}

function Connect-TargetPnP {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url
    )

    if (-not (Get-Module -ListAvailable -Name PnP.PowerShell)) {
        throw "PnP.PowerShell module is not installed. Install-Module PnP.PowerShell -Scope CurrentUser"
    }

    $securePassword = ConvertTo-SecureString $TargetPasswordPlain -AsPlainText -Force
    $targetCredential = New-Object System.Management.Automation.PSCredential($TargetUserName, $securePassword)

    Connect-PnPOnline -Url $Url -Credentials $targetCredential -ClientId $TargetClientId
    return Get-PnPConnection
}

function Get-ListTypeLabel {
    param(
        [Parameter(Mandatory = $true)]
        [object]$BaseType
    )

    if ($BaseType.ToString() -eq "DocumentLibrary") {
        return "DocumentLibrary"
    }

    return "List"
}

function Get-SourceListsInfo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context
    )

    $web = $Context.Web
    $lists = $web.Lists
    $Context.Load(
        $lists,
        [System.Linq.Expressions.Expression[Func[Microsoft.SharePoint.Client.ListCollection, object]]]{
            param($lc)
            $lc.Include(
                {
                    param($l)
                    $l.Title
                },
                {
                    param($l)
                    $l.Hidden
                },
                {
                    param($l)
                    $l.ItemCount
                },
                {
                    param($l)
                    $l.BaseType
                }
            )
        }
    )
    $Context.ExecuteQuery()

    $result = @{}
    foreach ($list in $lists) {
        if ($list.Hidden) {
            continue
        }

        $result[$list.Title.ToLowerInvariant()] = [PSCustomObject]@{
            Title = $list.Title
            Type = Get-ListTypeLabel -BaseType $list.BaseType
            ItemCount = [int]$list.ItemCount
        }
    }

    return $result
}

function Get-TargetListsInfo {
    [CmdletBinding()]
    param()

    $lists = Get-PnPList -Includes Title, Hidden, ItemCount, BaseType

    $result = @{}
    foreach ($list in $lists) {
        if ($list.Hidden) {
            continue
        }

        $result[$list.Title.ToLowerInvariant()] = [PSCustomObject]@{
            Title = $list.Title
            Type = Get-ListTypeLabel -BaseType $list.BaseType
            ItemCount = [int]$list.ItemCount
        }
    }

    return $result
}

function Expand-RoleAssignments {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientObjectCollection[Microsoft.SharePoint.Client.RoleAssignment]]$RoleAssignments,

        [Parameter(Mandatory = $true)]
        [object]$Context,

        [Parameter(Mandatory = $true)]
        [string]$ScopeType,

        [Parameter(Mandatory = $true)]
        [string]$ScopeName,

        [Parameter(Mandatory = $true)]
        [switch]$IsPnP
    )

    if ($IsPnP) {
        Get-PnPProperty -ClientObject $RoleAssignments
    }
    else {
        $Context.Load($RoleAssignments)
        $Context.ExecuteQuery()
    }

    $items = @()
    foreach ($ra in $RoleAssignments) {
        if ($IsPnP) {
            Get-PnPProperty -ClientObject $ra -Property Member, RoleDefinitionBindings
            Get-PnPProperty -ClientObject $ra.RoleDefinitionBindings
        }
        else {
            $Context.Load($ra.Member)
            $Context.Load($ra.RoleDefinitionBindings)
            $Context.ExecuteQuery()
        }

        $roleNames = @($ra.RoleDefinitionBindings | ForEach-Object { $_.Name } | Sort-Object -Unique)
        $items += [PSCustomObject]@{
            ScopeType = $ScopeType
            ScopeName = $ScopeName
            Principal = $ra.Member.Title
            Roles = ($roleNames -join ";")
        }
    }

    return $items
}

function Get-SourcePermissions {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context
    )

    $web = $Context.Web
    $Context.Load($web, "Title", "RoleAssignments", "Lists")
    $Context.ExecuteQuery()

    $permissions = @()
    $permissions += Expand-RoleAssignments -RoleAssignments $web.RoleAssignments -Context $Context -ScopeType "Web" -ScopeName $web.Title

    $Context.Load(
        $web.Lists,
        [System.Linq.Expressions.Expression[Func[Microsoft.SharePoint.Client.ListCollection, object]]]{
            param($lc)
            $lc.Include(
                {
                    param($l)
                    $l.Title
                },
                {
                    param($l)
                    $l.Hidden
                },
                {
                    param($l)
                    $l.RoleAssignments
                }
            )
        }
    )
    $Context.ExecuteQuery()

    foreach ($list in $web.Lists) {
        if ($list.Hidden) {
            continue
        }

        $permissions += Expand-RoleAssignments -RoleAssignments $list.RoleAssignments -Context $Context -ScopeType "List" -ScopeName $list.Title
    }

    return $permissions
}

function Get-TargetPermissions {
    [CmdletBinding()]
    param()

    $web = Get-PnPWeb -Includes Title, RoleAssignments
    $permissions = @()

    $permissions += Expand-RoleAssignments -RoleAssignments $web.RoleAssignments -Context $null -ScopeType "Web" -ScopeName $web.Title -IsPnP

    $lists = Get-PnPList -Includes Title, Hidden, RoleAssignments
    foreach ($list in $lists) {
        if ($list.Hidden) {
            continue
        }

        $permissions += Expand-RoleAssignments -RoleAssignments $list.RoleAssignments -Context $null -ScopeType "List" -ScopeName $list.Title -IsPnP
    }

    return $permissions
}

function Add-NavNodesFromCollection {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [object]$Context,

        [Parameter(Mandatory = $true)]
        [object]$NodeCollection,

        [Parameter(Mandatory = $true)]
        [string]$Location,

        [Parameter(Mandatory = $true)]
        [string]$Parent,

        [Parameter(Mandatory = $true)]
        [switch]$IsPnP,

        [Parameter(Mandatory = $true)]
        [ref]$Accumulator
    )

    if ($IsPnP) {
        Get-PnPProperty -ClientObject $NodeCollection
    }
    else {
        $Context.Load($NodeCollection)
        $Context.ExecuteQuery()
    }

    foreach ($node in $NodeCollection) {
        if ($IsPnP) {
            Get-PnPProperty -ClientObject $node -Property Title, Url, Children
        }
        else {
            $Context.Load($node, "Title", "Url", "Children")
            $Context.ExecuteQuery()
        }

        $Accumulator.Value += [PSCustomObject]@{
            Location = $Location
            Parent = $Parent
            Title = $node.Title
            Url = $node.Url
        }

        Add-NavNodesFromCollection -Context $Context -NodeCollection $node.Children -Location $Location -Parent $node.Title -IsPnP:$IsPnP -Accumulator $Accumulator
    }
}

function Get-SourceNavigation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.SharePoint.Client.ClientContext]$Context
    )

    $web = $Context.Web
    $Context.Load($web, "Navigation")
    $Context.Load($web.Navigation, "TopNavigationBar", "QuickLaunch")
    $Context.ExecuteQuery()

    $nodes = @()
    Add-NavNodesFromCollection -Context $Context -NodeCollection $web.Navigation.TopNavigationBar -Location "TopNavigationBar" -Parent "ROOT" -Accumulator ([ref]$nodes)
    Add-NavNodesFromCollection -Context $Context -NodeCollection $web.Navigation.QuickLaunch -Location "QuickLaunch" -Parent "ROOT" -Accumulator ([ref]$nodes)

    return $nodes
}

function Get-TargetNavigation {
    [CmdletBinding()]
    param()

    $ctx = (Get-PnPConnection).Context
    $top = Get-PnPNavigationNode -Location TopNavigationBar
    $quick = Get-PnPNavigationNode -Location QuickLaunch

    $nodes = @()
    Add-NavNodesFromCollection -Context $ctx -NodeCollection $top -Location "TopNavigationBar" -Parent "ROOT" -Accumulator ([ref]$nodes) -IsPnP
    Add-NavNodesFromCollection -Context $ctx -NodeCollection $quick -Location "QuickLaunch" -Parent "ROOT" -Accumulator ([ref]$nodes) -IsPnP

    return $nodes
}

function Compare-KeyedObjects {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Source,

        [Parameter(Mandatory = $true)]
        [hashtable]$Target
    )

    $rows = @()

    foreach ($key in $Source.Keys) {
        if (-not $Target.ContainsKey($key)) {
            $rows += [PSCustomObject]@{
                Entity = $Source[$key].Title
                Status = "MissingInTarget"
                Detail = "List or library exists in source but not in target."
            }
            continue
        }

        $sourceItem = $Source[$key]
        $targetItem = $Target[$key]

        if (($sourceItem.Type -eq $targetItem.Type) -and ($sourceItem.ItemCount -eq $targetItem.ItemCount)) {
            $rows += [PSCustomObject]@{
                Entity = $sourceItem.Title
                Status = "Match"
                Detail = "Type and item count match. Source=$($sourceItem.ItemCount), Target=$($targetItem.ItemCount)"
            }
        }
        else {
            $rows += [PSCustomObject]@{
                Entity = $sourceItem.Title
                Status = "Mismatch"
                Detail = "Type/ItemCount mismatch. SourceType=$($sourceItem.Type), TargetType=$($targetItem.Type), SourceItems=$($sourceItem.ItemCount), TargetItems=$($targetItem.ItemCount)"
            }
        }
    }

    foreach ($key in $Target.Keys) {
        if (-not $Source.ContainsKey($key)) {
            $rows += [PSCustomObject]@{
                Entity = $Target[$key].Title
                Status = "ExtraInTarget"
                Detail = "List or library exists in target but not in source."
            }
        }
    }

    return $rows | Sort-Object Entity, Status
}

function Compare-PermissionRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$SourceRows,

        [Parameter(Mandatory = $true)]
        [array]$TargetRows
    )

    $srcMap = @{}
    foreach ($r in $SourceRows) {
        $k = "{0}|{1}|{2}" -f $r.ScopeType.ToLowerInvariant(), $r.ScopeName.ToLowerInvariant(), $r.Principal.ToLowerInvariant()
        $srcMap[$k] = $r
    }

    $tgtMap = @{}
    foreach ($r in $TargetRows) {
        $k = "{0}|{1}|{2}" -f $r.ScopeType.ToLowerInvariant(), $r.ScopeName.ToLowerInvariant(), $r.Principal.ToLowerInvariant()
        $tgtMap[$k] = $r
    }

    $results = @()

    foreach ($k in $srcMap.Keys) {
        if (-not $tgtMap.ContainsKey($k)) {
            $results += [PSCustomObject]@{
                ScopeType = $srcMap[$k].ScopeType
                ScopeName = $srcMap[$k].ScopeName
                Principal = $srcMap[$k].Principal
                Status = "MissingInTarget"
                Detail = "Permission principal missing in target. Roles(Source)=$($srcMap[$k].Roles)"
            }
            continue
        }

        if ($srcMap[$k].Roles -eq $tgtMap[$k].Roles) {
            $results += [PSCustomObject]@{
                ScopeType = $srcMap[$k].ScopeType
                ScopeName = $srcMap[$k].ScopeName
                Principal = $srcMap[$k].Principal
                Status = "Match"
                Detail = "Roles match: $($srcMap[$k].Roles)"
            }
        }
        else {
            $results += [PSCustomObject]@{
                ScopeType = $srcMap[$k].ScopeType
                ScopeName = $srcMap[$k].ScopeName
                Principal = $srcMap[$k].Principal
                Status = "Mismatch"
                Detail = "Roles mismatch. Source=$($srcMap[$k].Roles), Target=$($tgtMap[$k].Roles)"
            }
        }
    }

    foreach ($k in $tgtMap.Keys) {
        if (-not $srcMap.ContainsKey($k)) {
            $results += [PSCustomObject]@{
                ScopeType = $tgtMap[$k].ScopeType
                ScopeName = $tgtMap[$k].ScopeName
                Principal = $tgtMap[$k].Principal
                Status = "ExtraInTarget"
                Detail = "Permission principal exists only in target. Roles(Target)=$($tgtMap[$k].Roles)"
            }
        }
    }

    return $results | Sort-Object ScopeType, ScopeName, Principal, Status
}

function Compare-NavigationRows {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [array]$SourceRows,

        [Parameter(Mandatory = $true)]
        [array]$TargetRows
    )

    $toKey = {
        param($n)
        return ("{0}|{1}|{2}|{3}" -f $n.Location.ToLowerInvariant(), $n.Parent.ToLowerInvariant(), $n.Title.ToLowerInvariant(), $n.Url.ToLowerInvariant())
    }

    $src = @{}
    foreach ($n in $SourceRows) { $src[(& $toKey $n)] = $n }

    $tgt = @{}
    foreach ($n in $TargetRows) { $tgt[(& $toKey $n)] = $n }

    $results = @()

    foreach ($k in $src.Keys) {
        if ($tgt.ContainsKey($k)) {
            $results += [PSCustomObject]@{
                Location = $src[$k].Location
                Parent = $src[$k].Parent
                Title = $src[$k].Title
                Url = $src[$k].Url
                Status = "Match"
            }
        }
        else {
            $results += [PSCustomObject]@{
                Location = $src[$k].Location
                Parent = $src[$k].Parent
                Title = $src[$k].Title
                Url = $src[$k].Url
                Status = "MissingInTarget"
            }
        }
    }

    foreach ($k in $tgt.Keys) {
        if (-not $src.ContainsKey($k)) {
            $results += [PSCustomObject]@{
                Location = $tgt[$k].Location
                Parent = $tgt[$k].Parent
                Title = $tgt[$k].Title
                Url = $tgt[$k].Url
                Status = "ExtraInTarget"
            }
        }
    }

    return $results | Sort-Object Location, Parent, Title, Url, Status
}

function Write-ComparisonSummary {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [array]$Rows,

        [Parameter(Mandatory = $true)]
        [string]$StatusPropertyName
    )

    $statusGroups = $Rows | Group-Object -Property $StatusPropertyName
    Write-Host "\n==== $Name ====" -ForegroundColor Cyan
    foreach ($group in $statusGroups) {
        Write-Host ("{0,-20}: {1}" -f $group.Name, $group.Count)
    }
}

try {
    Import-CSOMAssemblies

    Write-Host "Connecting source (CSOM using current logged-in user): $SourceSiteUrl" -ForegroundColor Yellow
    $sourceContext = Get-SourceContext -Url $SourceSiteUrl

    Write-Host "Connecting target (PnP with hardcoded credential + client id): $TargetSiteUrl" -ForegroundColor Yellow
    Connect-TargetPnP -Url $TargetSiteUrl | Out-Null

    Write-Host "Collecting source lists/libraries..." -ForegroundColor Yellow
    $sourceLists = Get-SourceListsInfo -Context $sourceContext

    Write-Host "Collecting target lists/libraries..." -ForegroundColor Yellow
    $targetLists = Get-TargetListsInfo

    Write-Host "Collecting source permissions..." -ForegroundColor Yellow
    $sourcePermissions = Get-SourcePermissions -Context $sourceContext

    Write-Host "Collecting target permissions..." -ForegroundColor Yellow
    $targetPermissions = Get-TargetPermissions

    Write-Host "Collecting source navigation..." -ForegroundColor Yellow
    $sourceNavigation = Get-SourceNavigation -Context $sourceContext

    Write-Host "Collecting target navigation..." -ForegroundColor Yellow
    $targetNavigation = Get-TargetNavigation

    Write-Host "Comparing lists and libraries..." -ForegroundColor Yellow
    $listComparison = Compare-KeyedObjects -Source $sourceLists -Target $targetLists

    Write-Host "Comparing permissions..." -ForegroundColor Yellow
    $permissionComparison = Compare-PermissionRows -SourceRows $sourcePermissions -TargetRows $targetPermissions

    Write-Host "Comparing navigation..." -ForegroundColor Yellow
    $navigationComparison = Compare-NavigationRows -SourceRows $sourceNavigation -TargetRows $targetNavigation

    if (-not (Test-Path $OutputFolder)) {
        New-Item -Path $OutputFolder -ItemType Directory -Force | Out-Null
    }

    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $listReportPath = Join-Path $OutputFolder "ListLibraryComparison_$stamp.csv"
    $permReportPath = Join-Path $OutputFolder "PermissionComparison_$stamp.csv"
    $navReportPath = Join-Path $OutputFolder "NavigationComparison_$stamp.csv"

    $listComparison | Export-Csv -Path $listReportPath -NoTypeInformation -Encoding UTF8
    $permissionComparison | Export-Csv -Path $permReportPath -NoTypeInformation -Encoding UTF8
    $navigationComparison | Export-Csv -Path $navReportPath -NoTypeInformation -Encoding UTF8

    Write-ComparisonSummary -Name "Lists/Libraries" -Rows $listComparison -StatusPropertyName "Status"
    Write-ComparisonSummary -Name "Permissions" -Rows $permissionComparison -StatusPropertyName "Status"
    Write-ComparisonSummary -Name "Navigation" -Rows $navigationComparison -StatusPropertyName "Status"

    Write-Host "\nReports generated:" -ForegroundColor Green
    Write-Host " - $listReportPath"
    Write-Host " - $permReportPath"
    Write-Host " - $navReportPath"
}
catch {
    Write-Error "Migration comparison failed: $($_.Exception.Message)"
    throw
}
finally {
    Disconnect-PnPOnline -ErrorAction SilentlyContinue
}

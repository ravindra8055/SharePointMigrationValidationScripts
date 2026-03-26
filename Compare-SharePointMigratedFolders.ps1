[CmdletBinding()]
param(
	[Parameter(Mandatory = $true)]
	[string]$SourceSiteUrl,

	[Parameter(Mandatory = $true)]
	[string]$TargetSiteUrl,

	[Parameter(Mandatory = $true)]
	[string]$CsvPath,

	[Parameter(Mandatory = $true)]
	[string]$OutputFolder,

	[int]$MaxOutputRecordsPerFile = 10000,

	[int]$PageSize = 2000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# SPO hardcoded authentication values
$TargetUserName = "hardcoded.user@tenant.onmicrosoft.com"
$TargetPasswordPlain = "HardcodedPasswordHere"
$TargetClientId = "00000000-0000-0000-0000-000000000000"
$script:PnPModuleMode = ""

function Import-CSOMAssemblies {
	[CmdletBinding()]
	param()

	if ("Microsoft.SharePoint.Client.ClientContext" -as [type]) {
		return
	}

	$candidatePaths = @(
		"C:\Program Files\Common Files\microsoft shared\Web Server Extensions\16\ISAPI",
		"C:\Program Files\Common Files\microsoft shared\Web Server Extensions\15\ISAPI"
	)

	foreach ($path in $candidatePaths) {
		$runtimeDll = Join-Path $path "Microsoft.SharePoint.Client.Runtime.dll"
		$clientDll = Join-Path $path "Microsoft.SharePoint.Client.dll"
		if ((Test-Path $runtimeDll) -and (Test-Path $clientDll)) {
			Add-Type -Path $runtimeDll
			Add-Type -Path $clientDll
			return
		}
	}

	throw "Unable to load CSOM assemblies. Install SharePoint CSOM or run this on a SharePoint server with ISAPI DLLs."
}

function Import-PnPModule {
	[CmdletBinding()]
	param()

	if ($PSVersionTable.PSVersion.Major -le 5) {
		if (Get-Module -ListAvailable -Name SharePointPnPPowerShellOnline) {
			Import-Module SharePointPnPPowerShellOnline -ErrorAction Stop
			$script:PnPModuleMode = "Legacy"
			return
		}

		if (Get-Module -ListAvailable -Name PnP.PowerShell) {
			Import-Module PnP.PowerShell -ErrorAction Stop
			$script:PnPModuleMode = "Modern"
			return
		}

		throw "No supported PnP module found. For Windows PowerShell 5.1 install SharePointPnPPowerShellOnline."
	}

	Import-Module PnP.PowerShell -ErrorAction Stop
	$script:PnPModuleMode = "Modern"
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

	$securePassword = ConvertTo-SecureString $TargetPasswordPlain -AsPlainText -Force
	$credential = New-Object System.Management.Automation.PSCredential($TargetUserName, $securePassword)
	$connectCommand = Get-Command Connect-PnPOnline -ErrorAction Stop

	if ($connectCommand.Parameters.ContainsKey("ClientId") -and -not [string]::IsNullOrWhiteSpace($TargetClientId)) {
		Connect-PnPOnline -Url $Url -Credentials $credential -ClientId $TargetClientId
	}
	else {
		Connect-PnPOnline -Url $Url -Credentials $credential
	}

	return (Get-PnPConnection).Context
}

function Disconnect-TargetPnP {
	[CmdletBinding()]
	param()

	if (Get-Command Disconnect-PnPOnline -ErrorAction SilentlyContinue) {
		Disconnect-PnPOnline -ErrorAction SilentlyContinue
	}
}

function Convert-ToServerRelativeUrl {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[string]$Url,

		[string]$SiteUrl
	)

	if ([string]::IsNullOrWhiteSpace($Url)) {
		throw "Folder URL is empty."
	}

	if ($Url.StartsWith("/")) {
		return $Url.TrimEnd("/")
	}

	$uriKind = [System.UriKind]::Absolute
	if ([Uri]::IsWellFormedUriString($Url, $uriKind)) {
		$uri = [Uri]$Url
		return $uri.AbsolutePath.TrimEnd("/")
	}

	if ([string]::IsNullOrWhiteSpace($SiteUrl)) {
		throw "Relative folder URL '$Url' requires SiteUrl to resolve to server-relative path."
	}

	$sitePath = ([Uri]$SiteUrl).AbsolutePath.TrimEnd("/")
	$combined = ($sitePath + "/" + $Url.TrimStart("/")).TrimEnd("/")
	return $combined
}

function Get-ListAndFolderContext {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[Microsoft.SharePoint.Client.ClientContext]$Context,

		[Parameter(Mandatory = $true)]
		[string]$FolderServerRelativeUrl
	)

	$folder = $Context.Web.GetFolderByServerRelativeUrl($FolderServerRelativeUrl)
	$Context.Load($folder)
	$Context.Load($folder.ListItemAllFields)
	$Context.Load($folder.ListItemAllFields.ParentList)
	$Context.Load($folder.ListItemAllFields.ParentList.RootFolder)
	$Context.ExecuteQuery()

	if (-not $folder.Exists) {
		throw "Folder does not exist: $FolderServerRelativeUrl"
	}

	return [PSCustomObject]@{
		FolderServerRelativeUrl = $folder.ServerRelativeUrl
		List = $folder.ListItemAllFields.ParentList
		ListTitle = $folder.ListItemAllFields.ParentList.Title
		ListRootServerRelativeUrl = $folder.ListItemAllFields.ParentList.RootFolder.ServerRelativeUrl
	}
}

function Get-RecursiveFolderInventory {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[Microsoft.SharePoint.Client.ClientContext]$Context,

		[Parameter(Mandatory = $true)]
		[Microsoft.SharePoint.Client.List]$List,

		[Parameter(Mandatory = $true)]
		[string]$RootFolderServerRelativeUrl,

		[Parameter(Mandatory = $true)]
		[int]$RowLimit
	)

	$inventory = @{}
	$position = $null

	do {
		$query = New-Object Microsoft.SharePoint.Client.CamlQuery
		$query.FolderServerRelativeUrl = $RootFolderServerRelativeUrl
		$query.ListItemCollectionPosition = $position
		$query.ViewXml = @"
<View Scope='RecursiveAll'>
	<ViewFields>
		<FieldRef Name='FileRef' />
		<FieldRef Name='FileLeafRef' />
		<FieldRef Name='FSObjType' />
		<FieldRef Name='Modified' />
	</ViewFields>
	<RowLimit Paged='TRUE'>$RowLimit</RowLimit>
</View>
"@

		$items = $List.GetItems($query)
		$Context.Load($items)
		$Context.ExecuteQuery()
		$position = $items.ListItemCollectionPosition

		foreach ($item in $items) {
			$fileRef = [string]$item["FileRef"]
			if ([string]::IsNullOrWhiteSpace($fileRef)) {
				continue
			}

			$isFolder = ([int]$item["FSObjType"] -eq 1)

			if ($fileRef.Equals($RootFolderServerRelativeUrl, [System.StringComparison]::OrdinalIgnoreCase)) {
				continue
			}

			$relativePath = $fileRef.Substring($RootFolderServerRelativeUrl.Length).TrimStart("/")
			if ([string]::IsNullOrWhiteSpace($relativePath)) {
				continue
			}

			$itemType = if ($isFolder) { "Folder" } else { "File" }
			$key = "{0}|{1}" -f $relativePath.ToLowerInvariant(), $itemType

			$inventory[$key] = [PSCustomObject]@{
				RelativePath = $relativePath
				Name = [string]$item["FileLeafRef"]
				ItemType = $itemType
				Url = $fileRef
				LastModified = [datetime]$item["Modified"]
			}
		}
	}
	while ($null -ne $position)

	return $inventory
}

function New-LogWriterState {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[string]$Folder,

		[Parameter(Mandatory = $true)]
		[int]$MaxRecords
	)

	if (-not (Test-Path $Folder)) {
		New-Item -ItemType Directory -Path $Folder -Force | Out-Null
	}

	return [PSCustomObject]@{
		OutputFolder = $Folder
		MaxRecords = $MaxRecords
		FileIndex = 0
		CurrentFilePath = ""
		CurrentFileRecordCount = 0
		Buffer = New-Object System.Collections.Generic.List[object]
		BufferFlushSize = 1000
	}
}

function Start-NewLogFile {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[psobject]$State
	)

	$State.FileIndex++
	$State.CurrentFileRecordCount = 0
	$State.Buffer.Clear()

	$name = "MissingItems_{0}_{1}.csv" -f (Get-Date -Format "yyyyMMdd_HHmmss"), $State.FileIndex.ToString("000")
	$State.CurrentFilePath = Join-Path $State.OutputFolder $name
}

function Write-LogBuffer {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[psobject]$State
	)

	if ($State.Buffer.Count -eq 0) {
		return
	}

	if (-not (Test-Path $State.CurrentFilePath)) {
		$State.Buffer | Export-Csv -Path $State.CurrentFilePath -NoTypeInformation
	}
	else {
		$State.Buffer | Export-Csv -Path $State.CurrentFilePath -NoTypeInformation -Append
	}

	$State.Buffer.Clear()
}

function Add-MissingRecord {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[psobject]$State,

		[Parameter(Mandatory = $true)]
		[psobject]$Record
	)

	if ([string]::IsNullOrWhiteSpace($State.CurrentFilePath)) {
		Start-NewLogFile -State $State
	}

	if ($State.CurrentFileRecordCount -ge $State.MaxRecords) {
		Write-LogBuffer -State $State
		Start-NewLogFile -State $State
	}

	$State.Buffer.Add($Record)
	$State.CurrentFileRecordCount++

	if ($State.Buffer.Count -ge $State.BufferFlushSize) {
		Write-LogBuffer -State $State
	}
}

function Get-FolderUrlMapping {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory = $true)]
		[psobject]$Row,

		[Parameter(Mandatory = $true)]
		[string]$SourceSite,

		[Parameter(Mandatory = $true)]
		[string]$TargetSite
	)

	$sourceCandidates = @("SourceFolderUrl", "SourceUrl", "FolderUrl", "Url")
	$targetCandidates = @("TargetFolderUrl", "DestinationFolderUrl", "TargetUrl", "DestinationUrl")

	$sourceUrl = $null
	foreach ($name in $sourceCandidates) {
		if ($Row.PSObject.Properties.Name -contains $name -and -not [string]::IsNullOrWhiteSpace([string]$Row.$name)) {
			$sourceUrl = [string]$Row.$name
			break
		}
	}

	if ([string]::IsNullOrWhiteSpace($sourceUrl)) {
		throw "CSV row missing source folder URL. Expected one of: $($sourceCandidates -join ', ')"
	}

	$targetUrl = $null
	foreach ($name in $targetCandidates) {
		if ($Row.PSObject.Properties.Name -contains $name -and -not [string]::IsNullOrWhiteSpace([string]$Row.$name)) {
			$targetUrl = [string]$Row.$name
			break
		}
	}

	if ([string]::IsNullOrWhiteSpace($targetUrl)) {
		$sourceRel = Convert-ToServerRelativeUrl -Url $sourceUrl -SiteUrl $SourceSite
		$sourceSiteRel = ([Uri]$SourceSite).AbsolutePath.TrimEnd("/")

		if (-not $sourceRel.StartsWith($sourceSiteRel, [System.StringComparison]::OrdinalIgnoreCase)) {
			throw "Source folder URL '$sourceUrl' is not under source site '$SourceSite'."
		}

		$tail = $sourceRel.Substring($sourceSiteRel.Length)
		$targetUrl = ($targetSite.TrimEnd("/")) + $tail
	}

	return [PSCustomObject]@{
		SourceFolderUrl = $sourceUrl
		TargetFolderUrl = $targetUrl
	}
}

$logState = $null
$summary = [PSCustomObject]@{
	TotalFolderRows = 0
	RowsAttempted = 0
	FoldersProcessedSuccessfully = 0
	FoldersFailed = 0
	MissingFiles = 0
	MissingFolders = 0
	FatalErrors = 0
}

try {
	$logState = New-LogWriterState -Folder $OutputFolder -MaxRecords $MaxOutputRecordsPerFile

	Import-CSOMAssemblies
	Import-PnPModule

	if (-not (Test-Path $CsvPath)) {
		throw "CSV file not found: $CsvPath"
	}

	$sourceContext = Get-SourceContext -Url $SourceSiteUrl
	$targetContext = Connect-TargetPnP -Url $TargetSiteUrl

	$rows = Import-Csv -Path $CsvPath
	if ($null -eq $rows -or $rows.Count -eq 0) {
		throw "CSV does not contain any folder rows."
	}

	$summary.TotalFolderRows = $rows.Count

	$rowIndex = 0
	foreach ($row in $rows) {
		$rowIndex++
		$summary.RowsAttempted++
		Write-Host "[$rowIndex/$($rows.Count)] Processing folder mapping..."

		$mapping = $null
		try {
			$mapping = Get-FolderUrlMapping -Row $row -SourceSite $SourceSiteUrl -TargetSite $TargetSiteUrl
				$sourceFolderRel = Convert-ToServerRelativeUrl -Url $mapping.SourceFolderUrl -SiteUrl $SourceSiteUrl
				$targetFolderRel = Convert-ToServerRelativeUrl -Url $mapping.TargetFolderUrl -SiteUrl $TargetSiteUrl

			$sourceFolderContext = Get-ListAndFolderContext -Context $sourceContext -FolderServerRelativeUrl $sourceFolderRel
			$targetFolderContext = Get-ListAndFolderContext -Context $targetContext -FolderServerRelativeUrl $targetFolderRel

			$sourceItems = Get-RecursiveFolderInventory -Context $sourceContext -List $sourceFolderContext.List -RootFolderServerRelativeUrl $sourceFolderContext.FolderServerRelativeUrl -RowLimit $PageSize
			$targetItems = Get-RecursiveFolderInventory -Context $targetContext -List $targetFolderContext.List -RootFolderServerRelativeUrl $targetFolderContext.FolderServerRelativeUrl -RowLimit $PageSize

			foreach ($sourceKey in $sourceItems.Keys) {
				if ($targetItems.ContainsKey($sourceKey)) {
					continue
				}

				$missing = $sourceItems[$sourceKey]
				if ($missing.ItemType -eq "File") {
					$summary.MissingFiles++
				}
				else {
					$summary.MissingFolders++
				}

				Add-MissingRecord -State $logState -Record ([PSCustomObject]@{
					SourceFolderUrl = $mapping.SourceFolderUrl
					TargetFolderUrl = $mapping.TargetFolderUrl
					ItemType = $missing.ItemType
					Name = $missing.Name
					SourceItemUrl = $missing.Url
					LastModifiedUtc = $missing.LastModified.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
					Reason = "Missing in target"
				})
			}

			$summary.FoldersProcessedSuccessfully++
		}
		catch {
			$summary.FoldersFailed++

			Add-MissingRecord -State $logState -Record ([PSCustomObject]@{
				SourceFolderUrl = if ($null -ne $mapping) { $mapping.SourceFolderUrl } else { "" }
				TargetFolderUrl = if ($null -ne $mapping) { $mapping.TargetFolderUrl } else { "" }
				ItemType = "Folder"
				Name = "ProcessingError"
				SourceItemUrl = ""
				LastModifiedUtc = ""
				Reason = $_.Exception.Message
			})

			Write-Warning "Failed processing row $rowIndex. $($_.Exception.Message)"
		}
	}
}
catch {
	$summary.FatalErrors++
	$fatalMessage = $_.Exception.Message

	Write-Warning "Fatal script error: $fatalMessage"

	if ($null -ne $logState) {
		try {
			Add-MissingRecord -State $logState -Record ([PSCustomObject]@{
				SourceFolderUrl = ""
				TargetFolderUrl = ""
				ItemType = "Script"
				Name = "FatalError"
				SourceItemUrl = ""
				LastModifiedUtc = ""
				Reason = $fatalMessage
			})
		}
		catch {
			Write-Warning "Could not add fatal error to log buffer. $($_.Exception.Message)"
		}
	}
}
finally {
	if ($null -ne $logState) {
		try {
			Write-LogBuffer -State $logState
		}
		catch {
			Write-Warning "Failed to flush log buffer. $($_.Exception.Message)"
		}
	}

	Write-Host ""
	Write-Host "Execution completed."
	Write-Host "Total folder rows                : $($summary.TotalFolderRows)"
	Write-Host "Rows attempted                   : $($summary.RowsAttempted)"
	Write-Host "Folders processed successfully   : $($summary.FoldersProcessedSuccessfully)"
	Write-Host "Folders failed                   : $($summary.FoldersFailed)"
	Write-Host "Missing files                    : $($summary.MissingFiles)"
	Write-Host "Missing folders                  : $($summary.MissingFolders)"
	Write-Host "Fatal errors                     : $($summary.FatalErrors)"
	Write-Host "Log files created                : $(if ($null -ne $logState) { $logState.FileIndex } else { 0 })"
	Write-Host "Output folder                    : $OutputFolder"

	try {
		Disconnect-TargetPnP
	}
	catch {
		Write-Warning "Disconnect-PnPOnline failed. $($_.Exception.Message)"
	}
}

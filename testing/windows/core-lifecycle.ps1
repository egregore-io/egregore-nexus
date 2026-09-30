$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$Repo = 'C:\w\src\egregore-nexus'
$Built = Join-Path $env:CARGO_TARGET_DIR 'release\nexus.exe'
$ExpectedVersion = (Get-Content "$Repo\VERSION" -Raw).Trim()
$Root = Join-Path $env:NEXUS_HOME 'package-smoke'
$Prefix = Join-Path $Root 'npm prefix with spaces'
$Packs = Join-Path $Root 'packs'
New-Item -ItemType Directory -Force $Prefix,$Packs | Out-Null
if (Get-ScheduledTask -TaskName EgregoreNexusDaemon -ErrorAction SilentlyContinue) {
    throw 'Pre-existing Nexus task in disposable guest; refuse to overwrite it'
}
foreach ($Name in @('NEXUS_NAME','NEXUS_CLIENT_KEY','NEXUS_SESSION_ID','NEXUS_AGENT','NEXUS_NATIVE_BIN')) {
    Remove-Item "Env:$Name" -ErrorAction SilentlyContinue
}
$env:NEXUS_NO_SETUP = '1'
if ($env:NEXUS_SMOKE_EXPLICIT_DB -eq '1') {
    $Db = (Join-Path $env:NEXUS_HOME 'nexus.db').Replace('\','/')
    Set-Content -LiteralPath (Join-Path $env:NEXUS_HOME 'nexus.toml') -Encoding utf8 -Value ("db_path = '" + $Db + "'")
    Write-Output 'DIAGNOSTIC: explicit database path; not default-install acceptance'
}
$NativeRoot = "$Repo\packages\nexus-cli\native\win32-x64-msvc"
New-Item -ItemType Directory -Force $NativeRoot | Out-Null
Copy-Item $Built "$NativeRoot\nexus.exe" -Force
$BuildHash = (Get-FileHash $Built -Algorithm SHA256).Hash
if ($env:NEXUS_EXPECTED_EXE_SHA256 -and $BuildHash -ne $env:NEXUS_EXPECTED_EXE_SHA256) { throw 'Unexpected Windows artifact identity' }
Push-Location "$Repo\packages\nexus-cli"
try {
    & npm.cmd pack --pack-destination $Packs
    if ($LASTEXITCODE -ne 0) { throw 'npm pack failed' }
} finally { Pop-Location }
$Tarballs = @(Get-ChildItem $Packs -Filter '*.tgz')
if ($Tarballs.Count -ne 1) { throw 'Expected exactly one CLI tarball' }
& npm.cmd install --global --prefix $Prefix --no-audit --no-fund --offline $Tarballs[0].FullName
if ($LASTEXITCODE -ne 0) { throw 'Offline local npm install failed' }
New-Item -ItemType Directory -Force 'C:\w\artifacts' | Out-Null
Copy-Item $Tarballs[0].FullName 'C:\w\artifacts\nexus-core-cli.tgz' -Force
Write-Output ('Packed tarball SHA256=' + (Get-FileHash $Tarballs[0].FullName -Algorithm SHA256).Hash)
$Launcher = Join-Path $Prefix 'nexus.cmd'
$Installed = Join-Path $Prefix 'node_modules\@egregore\nexus-cli\native\win32-x64-msvc\nexus.exe'
if ((Get-FileHash $Installed -Algorithm SHA256).Hash -ne $BuildHash) { throw 'Installed native bytes differ' }
function Invoke-Nexus([string[]] $Arguments, [bool] $ExpectFailure = $false) {
    # Do not use a PowerShell native pipeline here: it waits for detached descendants too.
    # Explicit process ownership bounds the CLI itself and preserves actual exit status.
    $Info = [System.Diagnostics.ProcessStartInfo]::new()
    $Info.FileName = (Get-Command node.exe).Source
    $Entry = Join-Path $Prefix 'node_modules\@egregore\nexus-cli\bin\nexus.mjs'
    $Info.Arguments = (@($Entry) + $Arguments | ForEach-Object { '"' + $_.Replace('"','\"') + '"' }) -join ' '
    $Info.UseShellExecute = $false
    $Info.RedirectStandardOutput = $true
    $Info.RedirectStandardError = $true
    $Process = [System.Diagnostics.Process]::new()
    $Process.StartInfo = $Info
    $Elapsed = [System.Diagnostics.Stopwatch]::StartNew()
    if (!$Process.Start()) { throw 'CLI failed to start' }
    $Output = $Process.StandardOutput.ReadToEndAsync()
    $ErrorOutput = $Process.StandardError.ReadToEndAsync()
    # Includes bounded registration plus scheduled startup; the delayed-launch assertion below
    # still requires actual native readiness and one action, not merely a longer fixture wait.
    if (!$Process.WaitForExit(120000)) { $Process.Kill(); throw "CLI timed out: $Arguments" }
    if (!$Output.Wait(3000) -or !$ErrorOutput.Wait(3000)) { throw "CLI retained output pipes: $Arguments" }
    $Text = $Output.Result + $ErrorOutput.Result
    $Code = $Process.ExitCode
    $Process.Dispose()
    Write-Host $Text
    Write-Host "CLI completed in $($Elapsed.ElapsedMilliseconds)ms: $Arguments"
    if ($ExpectFailure) {
        if ($Code -eq 0 -or $Text -notmatch 'did not become ready') { throw "Expected readiness refusal: $Text" }
    } else {
        if ($Code -ne 0) { throw "Nexus failed: $Arguments (exit $Code): $Text" }
        if ($Text -match 'started pid=unknown') { throw 'Start reported success without a confirmed PID' }
    }
}
function Confirm-Healthy {
    $Deadline = [DateTime]::UtcNow.AddSeconds(15)
    do {
        $StatusText = (& $Launcher daemon status 2>&1 | Out-String)
        $StatusExit = $LASTEXITCODE
        if ($StatusExit -eq 0) { break }
        Start-Sleep -Milliseconds 100
    } while ([DateTime]::UtcNow -lt $Deadline)
    if ($StatusExit -ne 0) { throw "Daemon never became healthy: $StatusText" }
    Write-Host $StatusText
    Invoke-Nexus @('--json','members')
    $DaemonPid = [int](Get-Content (Join-Path $env:NEXUS_HOME 'daemon.pid') -Raw).Trim()
    $Process = Get-Process -Id $DaemonPid -ErrorAction Stop
    if ($Process.Path -ne $Installed) { throw "Wrong daemon binary: $($Process.Path)" }
    return $DaemonPid
}
function Confirm-Stopped([int] $PreviousPid, [bool] $ExpectCleanup = $false) {
    $Deadline = [DateTime]::UtcNow.AddSeconds(15)
    while ((Get-Process -Id $PreviousPid -ErrorAction SilentlyContinue) -and [DateTime]::UtcNow -lt $Deadline) {
        Start-Sleep -Milliseconds 100
    }
    if (Get-Process -Id $PreviousPid -ErrorAction SilentlyContinue) { throw "Daemon $PreviousPid survived stop" }
    if ($ExpectCleanup) {
        foreach ($Name in @('daemon.pid','daemon.lock','daemon-ipc-endpoint.json')) {
            if (Test-Path (Join-Path $env:NEXUS_HOME $Name)) { throw "Shutdown did not clean $Name" }
        }
    }
}
$Version = (& $Launcher --version | Out-String).Trim()
if ($LASTEXITCODE -ne 0 -or $Version -ne "nexus $ExpectedVersion") { throw "Installed version mismatch: $Version" }
Write-Output "Installed exact $Version SHA256=$BuildHash"
$Pids = @()
$InstalledTask = $false
try {
    Invoke-Nexus @('daemon','start')
    $Pids += @(Confirm-Healthy)[-1]
    Invoke-Nexus @('daemon','restart')
    Confirm-Stopped $Pids[-1]
    $Pids += @(Confirm-Healthy)[-1]
    Invoke-Nexus @('daemon','stop')
    Confirm-Stopped $Pids[-1] $true
    $InstalledTask = $true
    Invoke-Nexus @('daemon','install','--binary',$Installed)
    $Task = Get-ScheduledTask -TaskName EgregoreNexusDaemon
    if ($Task.Principal.RunLevel -ne 'Limited') { throw 'Daemon task unexpectedly elevated' }
    $Pids += @(Confirm-Healthy)[-1]
    # A scheduler-accepted action can begin its native child after the detached-start budget.
    # Exercise that exact boundary without retrying /Run or changing the native executable.
    Invoke-Nexus @('daemon','stop')
    Confirm-Stopped $Pids[-1]
    $WrapperPath = Join-Path $env:NEXUS_HOME 'daemon-task.ps1'
    $WrapperBytes = [IO.File]::ReadAllBytes($WrapperPath)
    $StartCount = Join-Path $env:NEXUS_HOME 'slow-task-start-count.txt'
    if (Test-Path $StartCount) { throw 'Unexpected slow-start marker' }
    try {
        $PrefixScript = "Add-Content -LiteralPath '" + $StartCount.Replace("'", "''") + "' -Value started`r`nStart-Sleep -Seconds 20`r`n"
        [IO.File]::WriteAllText($WrapperPath, $PrefixScript + [IO.File]::ReadAllText($WrapperPath), [Text.UTF8Encoding]::new($true))
        $SlowStart = [Diagnostics.Stopwatch]::StartNew()
        Invoke-Nexus @('daemon','start')
        if ($SlowStart.ElapsedMilliseconds -lt 20000) { throw 'Slow task was reported ready before native launch' }
        $Pids += @(Confirm-Healthy)[-1]
        if (@(Get-Content $StartCount).Count -ne 1) { throw 'Scheduled startup was retried' }
        Write-Output 'PASS delayed scheduled launch requires native readiness without relaunch'
    } finally {
        try {
            Invoke-Nexus @('daemon','stop')
        } finally {
            [IO.File]::WriteAllBytes($WrapperPath, $WrapperBytes)
        }
    }
    Confirm-Stopped $Pids[-1]
    Invoke-Nexus @('daemon','start')
    $Pids += @(Confirm-Healthy)[-1]
    Invoke-Nexus @('daemon','restart')
    Confirm-Stopped $Pids[-1]
    $Pids += @(Confirm-Healthy)[-1]
    # Repeat the previously intermittent stop/start boundary; no sleeps or retries of /Run.
    for ($Cycle=0; $Cycle -lt 10; $Cycle++) {
        Invoke-Nexus @('daemon','stop')
        Confirm-Stopped $Pids[-1]
        Invoke-Nexus @('daemon','start')
        $Pids += @(Confirm-Healthy)[-1]
    }
    Invoke-Nexus @('daemon','stop')
    Confirm-Stopped $Pids[-1]
    # Actual boot failure must be non-success and leave diagnostics, not "started pid=unknown".
    $BadDb = Join-Path $env:NEXUS_HOME 'intentional-invalid-db-directory'
    New-Item -ItemType Directory -Force $BadDb | Out-Null
    $ConfigPath = Join-Path $env:NEXUS_HOME 'nexus.toml'
    if (Test-Path $ConfigPath) { throw 'Unexpected configuration before disposable negative control' }
    # Isolate the first failed native exit from the scheduler's one-minute restart timer. The
    # production settings were used above; restore the captured settings after this negative probe.
    $OriginalSettings = (Get-ScheduledTask -TaskName EgregoreNexusDaemon).Settings
    $ProbeSettings = (Get-ScheduledTask -TaskName EgregoreNexusDaemon).Settings
    $ProbeSettings.RestartInterval = 'PT5M'
    try {
        Set-ScheduledTask -TaskName EgregoreNexusDaemon -Settings $ProbeSettings | Out-Null
        Set-Content -LiteralPath $ConfigPath -Encoding utf8 -Value ("db_path = '" + $BadDb.Replace('\','/') + "'")
        Invoke-Nexus @('daemon','start') $true
        $BootLog = Get-Content (Join-Path $env:NEXUS_HOME 'daemon.log') -Raw
        if ($BootLog -notmatch 'Failed to connect to database|Unable to open connection') { throw 'Scheduled boot failure was not captured in daemon.log' }
        $Task = Get-ScheduledTask -TaskName EgregoreNexusDaemon
        $TaskInfo = Get-ScheduledTaskInfo -TaskName EgregoreNexusDaemon
        if ($Task.State -ne 'Ready' -or $TaskInfo.LastTaskResult -ne 1) {
            throw "Expected completed native exit1, got state=$($Task.State) result=$($TaskInfo.LastTaskResult)"
        }
    } finally {
        try {
            Invoke-Nexus @('daemon','stop')
        } finally {
            try {
                if (Test-Path $ConfigPath) { Remove-Item -LiteralPath $ConfigPath }
            } finally {
                Set-ScheduledTask -TaskName EgregoreNexusDaemon -Settings $OriginalSettings | Out-Null
            }
        }
    }
    Invoke-Nexus @('daemon','start')
    $Pids += @(Confirm-Healthy)[-1]
    Invoke-Nexus @('daemon','uninstall')
    $InstalledTask = $false
    Confirm-Stopped $Pids[-1]
    if (Get-ScheduledTask -TaskName EgregoreNexusDaemon -ErrorAction SilentlyContinue) { throw 'Task remains after uninstall' }
    Write-Output 'PASS exact packed Windows CLI install, named-pipe reads, detached lifecycle and scheduled-task lifecycle'
} catch {
    # Preserve manager state before fixture cleanup; do not add timing delays to the tested path.
    Get-ScheduledTask -TaskName EgregoreNexusDaemon -ErrorAction SilentlyContinue |
        Select-Object TaskName,State | Format-List | Out-String | Write-Output
    Get-ScheduledTaskInfo -TaskName EgregoreNexusDaemon -ErrorAction SilentlyContinue |
        Select-Object LastRunTime,LastTaskResult,NextRunTime | Format-List | Out-String | Write-Output
    Get-Process nexus,powershell -ErrorAction SilentlyContinue |
        Select-Object Id,ProcessName,StartTime | Format-Table | Out-String | Write-Output
    throw
} finally {
    if ($InstalledTask) { & $Launcher daemon uninstall }
    $PidFile = Join-Path $env:NEXUS_HOME 'daemon.pid'
    if (Test-Path $PidFile) { $Pids += [int](Get-Content $PidFile -Raw).Trim() }
    foreach ($Id in $Pids) {
        $Process = Get-Process -Id $Id -ErrorAction SilentlyContinue
        if ($Process -and $Process.Path -eq $Installed) { Stop-Process -Id $Id -Force }
    }
}

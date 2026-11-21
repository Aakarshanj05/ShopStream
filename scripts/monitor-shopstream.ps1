<#
.SYNOPSIS
  Read-only host, Docker, WSL2, PostgreSQL, and Airflow sampler for one ShopStream test.

.DESCRIPTION
  Writes JSONL and text samples under diagnostics/<RunId>. Does not change service,
  database, DAG, WSL, Docker, or project data. Default interval is 30 seconds.

.EXAMPLE
  .\scripts\monitor-shopstream.ps1
  .\scripts\monitor-shopstream.ps1 -RunId publication-20261108-a -IntervalSeconds 30
  Stop with Ctrl+C in the monitor window. A final manifest is written on exit.
#>
[CmdletBinding()]
param(
    [ValidateRange(10, 300)]
    [int]$IntervalSeconds = 30,
    [string]$RunId,
    [string]$AirflowApiUrl = 'http://localhost:8082/api/v2/monitor/health'
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = 'diag-{0}-{1}' -f (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'), ([guid]::NewGuid().ToString('N').Substring(0, 8))
}
if ($RunId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$') {
    throw 'RunId may contain only letters, digits, period, underscore, and hyphen (maximum 80 characters).'
}

$outputDirectory = Join-Path (Join-Path $PSScriptRoot '..\diagnostics') $RunId
$outputDirectory = [System.IO.Path]::GetFullPath($outputDirectory)
if (Test-Path -LiteralPath $outputDirectory) {
    throw "Diagnostic output directory already exists; choose another RunId: $outputDirectory"
}
New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null

$manifestPath = Join-Path $outputDirectory 'manifest.json'
$stopFilePath = Join-Path $outputDirectory 'stop.requested'
$windowsPath = Join-Path $outputDirectory 'windows.jsonl'
$dockerStatsPath = Join-Path $outputDirectory 'docker-containers.jsonl'
$wslPath = Join-Path $outputDirectory 'wsl2-samples.log'
$postgresPath = Join-Path $outputDirectory 'postgres-health.jsonl'
$postgresLogsPath = Join-Path $outputDirectory 'postgres-logs.log'
$airflowPath = Join-Path $outputDirectory 'airflow-health.jsonl'
$airflowLogsPath = Join-Path $outputDirectory 'airflow-logs.log'
$errorsPath = Join-Path $outputDirectory 'monitor-errors.jsonl'

$manifest = [ordered]@{
    run_id = $RunId
    computer = $env:COMPUTERNAME
    started_utc = (Get-Date).ToUniversalTime().ToString('o')
    stopped_utc = $null
    interval_seconds = $IntervalSeconds
    airflow_api_url = $AirflowApiUrl
    output_directory = $outputDirectory
    stop_file = $stopFilePath
    stop_procedure = 'Press Ctrl+C in this PowerShell window, or create stop.requested in this run directory; no service stop command is used.'
}
$manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

function Write-JsonLine {
    param([string]$Path, [object]$Value)
    $Value | ConvertTo-Json -Compress -Depth 7 | Add-Content -LiteralPath $Path -Encoding UTF8
}

function Write-ErrorSample {
    param([string]$Source, [string]$Message)
    Write-JsonLine -Path $errorsPath -Value ([ordered]@{
        timestamp_utc = (Get-Date).ToUniversalTime().ToString('o')
        source = $Source
        error = $Message
    })
}

function Protect-LogText {
    param([string]$Text)
    $safe = [regex]::Replace($Text, '(?i)(://[^:/\s]+:)[^@\s/]+@', '${1}[REDACTED]@')
    $safe = [regex]::Replace($safe, '(?i)((?:password|passwd|secret|token|access[_-]?key|api[_-]?key)\s*[:=]\s*)("[^"]*"|''[^'']*''|[^,\s;]+)', '${1}[REDACTED]')
    $safe
}

function Invoke-TextCommand {
    param([string]$FilePath, [string[]]$ArgumentList)
    $text = (& $FilePath @ArgumentList 2>&1 | Out-String)
    $code = $LASTEXITCODE
    # Some Windows PowerShell builds surface WSL's UTF-16 console text as NUL-separated text.
    if ($text.IndexOf([char]0) -ge 0) { $text = $text.Replace([string][char]0, '') }
    [pscustomobject]@{ Text = $text.TrimEnd(); ExitCode = $code }
}

function Get-ComposeContainerIds {
    param([string]$Service)
    $result = Invoke-TextCommand -FilePath 'docker' -ArgumentList @(
        'ps', '-aq', '--filter', 'label=com.docker.compose.project=shopstream-airflow',
        '--filter', "label=com.docker.compose.service=$Service"
    )
    if ($result.ExitCode -ne 0) { throw "docker ps failed: $($result.Text)" }
    @($result.Text -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-ComposeStateText {
    param([string[]]$Ids)
    if (-not $Ids -or $Ids.Count -eq 0) { return '' }
    $format = '{{.Id}}|{{.Name}}|{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}|{{.State.FinishedAt}}'
    $inspectArguments = @('inspect', '--format', $format) + $Ids
    $result = Invoke-TextCommand -FilePath 'docker' -ArgumentList $inspectArguments
    if ($result.ExitCode -ne 0) { throw $result.Text }
    $result.Text
}

function Add-RecentDockerLogs {
    param([string]$ContainerId, [string]$Label, [datetime]$SinceUtc, [string]$Path)
    if ([string]::IsNullOrWhiteSpace($ContainerId)) { return }
    $sinceText = $SinceUtc.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    $result = Invoke-TextCommand -FilePath 'docker' -ArgumentList @('logs', '--timestamps', '--since', $sinceText, $ContainerId)
    if ($result.ExitCode -eq 0 -and $result.Text) {
        $safeLogs = Protect-LogText -Text $result.Text
        Add-Content -LiteralPath $Path -Value ("`n===== {0} sampled {1} =====`n{2}" -f $Label, (Get-Date).ToUniversalTime().ToString('o'), $safeLogs) -Encoding UTF8
    } elseif ($result.ExitCode -ne 0) {
        Write-ErrorSample -Source "docker-logs:$Label" -Message $result.Text
    }
}

$lastLogPollUtc = (Get-Date).ToUniversalTime().AddSeconds(-$IntervalSeconds)
$sampleNumber = 0
Write-Output "Monitoring run $RunId"
Write-Output "Output: $outputDirectory"
Write-Output "Interval: ${IntervalSeconds}s. Stop with Ctrl+C or create: $stopFilePath"

try {
    while ($true) {
        if (Test-Path -LiteralPath $stopFilePath) {
            Write-Output 'Stop marker found; finishing this sample loop.'
            break
        }
        $now = (Get-Date).ToUniversalTime()
        $stamp = $now.ToString('o')
        $sampleNumber++

        try {
            $memory = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfOS_Memory -ErrorAction Stop
            $processor = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfOS_Processor -Filter "Name='_Total'" -ErrorAction Stop
            $physicalDisk = Get-CimInstance -ClassName Win32_PerfFormattedData_PerfDisk_PhysicalDisk -Filter "Name='_Total'" -ErrorAction Stop
            Write-JsonLine -Path $windowsPath -Value ([ordered]@{
                timestamp_utc = $stamp
                available_memory_mb = [double]$memory.AvailableMBytes
                cpu_percent = [double]$processor.PercentProcessorTime
                disk_avg_latency_ms = [math]::Round(([double]$physicalDisk.AvgDisksecPerTransfer * 1000), 3)
                disk_current_queue_length = [double]$physicalDisk.CurrentDiskQueueLength
            })
        } catch { Write-ErrorSample -Source 'windows-counters' -Message $_.Exception.Message }

        try {
            $statsResult = Invoke-TextCommand -FilePath 'docker' -ArgumentList @('stats', '--no-stream', '--format', '{{.Container}}|{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}')
            if ($statsResult.ExitCode -ne 0) { throw $statsResult.Text }
            foreach ($line in ($statsResult.Text -split "`r?`n" | Where-Object { $_ })) {
                $parts = $line -split '\|', 5
                if ($parts.Count -eq 5) {
                    Write-JsonLine -Path $dockerStatsPath -Value ([ordered]@{
                        timestamp_utc = $stamp; container_id = $parts[0]; name = $parts[1]
                        cpu_percent = $parts[2]; memory_usage = $parts[3]; memory_percent = $parts[4]
                    })
                }
            }
            $allIdsResult = Invoke-TextCommand -FilePath 'docker' -ArgumentList @('ps', '-aq')
            if ($allIdsResult.ExitCode -ne 0) { throw $allIdsResult.Text }
            $allIds = @($allIdsResult.Text -split "`r?`n" | Where-Object { $_ })
            if ($allIds.Count -gt 0) {
                $stateResult = Invoke-TextCommand -FilePath 'docker' -ArgumentList (@('inspect', '--format', '{{.Id}}|{{.Name}}|{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}|{{.State.FinishedAt}}') + $allIds)
                if ($stateResult.ExitCode -ne 0) { throw $stateResult.Text }
                foreach ($line in ($stateResult.Text -split "`r?`n" | Where-Object { $_ })) {
                    $p = $line -split '\|', 7
                    if ($p.Count -eq 7) {
                        Write-JsonLine -Path $dockerStatsPath -Value ([ordered]@{
                            timestamp_utc = $stamp; kind = 'container_state'; container_id = $p[0]
                            name = $p[1]; status = $p[2]; exit_code = $p[3]; oom_killed = $p[4]
                            started_at = $p[5]; finished_at = $p[6]
                        })
                    }
                }
            }
        } catch { Write-ErrorSample -Source 'docker' -Message $_.Exception.Message }

        try {
            $wslCommand = 'date -u +%Y-%m-%dT%H:%M:%SZ; echo ===free===; free -b; echo ===pressure===; echo ---cpu---; cat /proc/pressure/cpu 2>&1; echo ---memory---; cat /proc/pressure/memory 2>&1; echo ---io---; cat /proc/pressure/io 2>&1; echo ===diskstats===; cat /proc/diskstats; echo ===kernel-tail===; dmesg -T 2>&1 | tail -n 60'
            $wslResult = Invoke-TextCommand -FilePath 'wsl.exe' -ArgumentList @('-d', 'docker-desktop', '-u', 'root', '--', 'sh', '-lc', $wslCommand)
            Add-Content -LiteralPath $wslPath -Value ("`n===== sample={0} timestamp_utc={1} exit={2} =====`n{3}" -f $sampleNumber, $stamp, $wslResult.ExitCode, $wslResult.Text) -Encoding UTF8
            if ($wslResult.ExitCode -ne 0) { Write-ErrorSample -Source 'wsl2' -Message $wslResult.Text }
        } catch { Write-ErrorSample -Source 'wsl2' -Message $_.Exception.Message }

        $dbIds = @()
        $apiIds = @()
        $schedulerIds = @()
        try {
            $dbIds = @(Get-ComposeContainerIds -Service 'airflow-db')
            $apiIds = @(Get-ComposeContainerIds -Service 'airflow-api-server')
            $schedulerIds = @(Get-ComposeContainerIds -Service 'airflow-scheduler')
        } catch { Write-ErrorSample -Source 'airflow-container-discovery' -Message $_.Exception.Message }
        try {
            $dbId = if ($dbIds.Count) { $dbIds[0] } else { '' }
            $apiId = if ($apiIds.Count) { $apiIds[0] } else { '' }
            $schedulerId = if ($schedulerIds.Count) { $schedulerIds[0] } else { '' }
            if ($dbId) {
                $ready = Invoke-TextCommand -FilePath 'docker' -ArgumentList @('exec', $dbId, 'pg_isready', '-U', 'airflow', '-d', 'airflow')
                $query = "select json_build_object('timestamp_utc', clock_timestamp() at time zone 'UTC', 'connections', count(*), 'active', count(*) filter (where state='active'), 'idle', count(*) filter (where state='idle')) from pg_stat_activity;"
                $dbMetrics = Invoke-TextCommand -FilePath 'docker' -ArgumentList @('exec', $dbId, 'psql', '-U', 'airflow', '-d', 'airflow', '-Atc', $query)
                Write-JsonLine -Path $postgresPath -Value ([ordered]@{
                    timestamp_utc = $stamp; container_id = $dbId
                    pg_isready_exit_code = $ready.ExitCode; pg_isready = $ready.Text
                    connection_metrics_exit_code = $dbMetrics.ExitCode; connection_metrics = $dbMetrics.Text
                })
                Add-RecentDockerLogs -ContainerId $dbId -Label 'postgres' -SinceUtc $lastLogPollUtc -Path $postgresLogsPath
            } else { Write-ErrorSample -Source 'postgres' -Message 'Airflow metadata DB container not found by Compose labels.' }
        } catch { Write-ErrorSample -Source 'postgres' -Message $_.Exception.Message }

        try {
            $apiResult = Invoke-WebRequest -Uri $AirflowApiUrl -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
            $apiBody = $apiResult.Content
            $apiStatus = [int]$apiResult.StatusCode
        } catch {
            $apiStatus = $null
            $apiBody = $_.Exception.Message
        }
        $apiState = ''
        $schedulerState = ''
        $containerStateError = $null
        try {
            $apiState = Get-ComposeStateText -Ids $apiIds
            $schedulerState = Get-ComposeStateText -Ids $schedulerIds
        } catch {
            $containerStateError = $_.Exception.Message
            Write-ErrorSample -Source 'airflow-container-state' -Message $containerStateError
        }
        Write-JsonLine -Path $airflowPath -Value ([ordered]@{
            timestamp_utc = $stamp; api_url = $AirflowApiUrl; api_http_status = $apiStatus
            api_response = $apiBody; api_container_state = $apiState; scheduler_container_state = $schedulerState
            container_state_error = $containerStateError
        })
        Add-RecentDockerLogs -ContainerId $(if ($apiIds.Count) { $apiIds[0] } else { '' }) -Label 'airflow-api' -SinceUtc $lastLogPollUtc -Path $airflowLogsPath
        Add-RecentDockerLogs -ContainerId $(if ($schedulerIds.Count) { $schedulerIds[0] } else { '' }) -Label 'airflow-scheduler' -SinceUtc $lastLogPollUtc -Path $airflowLogsPath

        $lastLogPollUtc = $now
        $elapsedSeconds = ((Get-Date).ToUniversalTime() - $now).TotalSeconds
        $sleepSeconds = [math]::Floor($IntervalSeconds - $elapsedSeconds)
        if ($sleepSeconds -gt 0) { Start-Sleep -Seconds $sleepSeconds }
    }
} finally {
    $manifest.stopped_utc = (Get-Date).ToUniversalTime().ToString('o')
    $manifest.samples_completed = $sampleNumber
    $manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $manifestPath -Encoding UTF8
    Write-Output "Stopped monitoring run $RunId; samples=$sampleNumber; output=$outputDirectory"
}

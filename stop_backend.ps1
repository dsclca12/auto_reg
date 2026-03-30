param(
    [int]$BackendPort = 8000,
    [int]$SolverPort = 8889,
    [int]$Grok2ApiPort = 8011,
    [int]$CLIProxyAPIPort = 8317,
    [int]$FullStop = 1
)

$ErrorActionPreference = "Stop"
$ports = @($BackendPort, $SolverPort)
if ($FullStop -ne 0) {
    $ports += @($Grok2ApiPort, $CLIProxyAPIPort)
}
$ports = $ports | Where-Object { $_ -gt 0 } | Select-Object -Unique

Write-Host "[INFO] 准备停止端口: $($ports -join ', ')"

function Get-ProcessIdsByPorts {
    param([int[]]$TargetPorts)
    $result = @()
    $connections = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -in $TargetPorts }
    foreach ($conn in $connections) {
        if ($conn.OwningProcess) {
            $result += [int]$conn.OwningProcess
        }
    }
    return $result | Select-Object -Unique
}

function Get-ProcessIdsByNames {
    param([string[]]$Names)
    $result = @()
    foreach ($name in $Names) {
        try {
            $items = Get-Process -Name $name -ErrorAction SilentlyContinue
            foreach ($item in $items) {
                $result += [int]$item.Id
            }
        } catch {}
    }
    return $result | Select-Object -Unique
}

function Wait-ProcessExit {
    param(
        [int]$ProcessId,
        [int]$TimeoutSeconds = 6
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) {
            return $true
        }
        Start-Sleep -Milliseconds 250
    }
    return -not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)
}

function Stop-ProcessTreeSafe {
    param([int]$ProcessId)

    if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) {
        return $true
    }

    Write-Host "[INFO] 尝试优雅停止 PID=$ProcessId"
    try {
        & taskkill.exe /PID $ProcessId /T *> $null
    } catch {
        Write-Warning "taskkill 优雅停止返回异常: $($_.Exception.Message)"
    }
    if (Wait-ProcessExit -ProcessId $ProcessId -TimeoutSeconds 6) {
        Write-Host "[OK] 已停止 PID=$ProcessId"
        return $true
    }

    Write-Warning "PID=$ProcessId 未在预期时间退出，改为强制停止"
    try {
        & taskkill.exe /PID $ProcessId /T /F *> $null
    } catch {
        Write-Warning "taskkill 强制停止返回异常: $($_.Exception.Message)"
    }
    if (Wait-ProcessExit -ProcessId $ProcessId -TimeoutSeconds 6) {
        Write-Host "[OK] 已强制停止 PID=$ProcessId"
        return $true
    }

    Write-Warning "taskkill 未能完全停止 PID=$ProcessId，尝试使用 Stop-Process -Force"
    try {
        Stop-Process -Id $ProcessId -Force -ErrorAction Stop
    } catch {
        Write-Warning "Stop-Process -Force 失败: $($_.Exception.Message)"
    }
    if (Wait-ProcessExit -ProcessId $ProcessId -TimeoutSeconds 6) {
        Write-Host "[OK] 已通过 Stop-Process 强制停止 PID=$ProcessId"
        return $true
    }

    Write-Warning "PID=$ProcessId 停止失败"
    return $false
}

$connections = Get-ProcessIdsByPorts -TargetPorts $ports
$extraNames = @()
if ($FullStop -ne 0) {
    $extraNames += @("KiroAccountManager", "kiro-account-manager")
}
$extraPids = Get-ProcessIdsByNames -Names $extraNames
$targets = @($connections + $extraPids) | Where-Object { $_ } | Select-Object -Unique

if (-not $targets) {
    Write-Host "[INFO] 未发现需要停止的进程"
    exit 0
}

foreach ($procId in $targets) {
    try {
        Stop-ProcessTreeSafe -ProcessId $procId | Out-Null
    } catch {
        Write-Warning "停止 PID=$procId 失败: $($_.Exception.Message)"
    }
}

Write-Host "[INFO] 停止完成"

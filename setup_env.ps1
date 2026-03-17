param(
    [string]$Python = "python",
    [string]$VenvPath = ".venv",
    [switch]$Recreate
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Get-Location
$TargetVenv = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot $VenvPath))
$PythonExe = Join-Path $TargetVenv "Scripts\python.exe"
$ActivateScript = Join-Path $TargetVenv "Scripts\Activate.ps1"
$ActiveVenv = $null

if ($env:VIRTUAL_ENV) {
    $ActiveVenv = [System.IO.Path]::GetFullPath($env:VIRTUAL_ENV)
}

if ($Recreate) {
    if ($ActiveVenv -and $ActiveVenv -ieq $TargetVenv) {
        throw "The target virtual environment is currently active. Deactivate it first, then rerun with -Recreate."
    }
    if (Test-Path $TargetVenv) {
        Remove-Item -Recurse -Force $TargetVenv
    }
}

if (-not (Test-Path $PythonExe)) {
    & $Python -m venv $TargetVenv
}
else {
    Write-Host "Reusing existing virtual environment: $TargetVenv"
}

if (-not (Test-Path $PythonExe)) {
    throw "Python virtual environment was not created successfully: $PythonExe"
}

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -r .\requirements.txt

Write-Host ""
Write-Host "Environment ready."
Write-Host "Activate with:"
Write-Host "  $ActivateScript"

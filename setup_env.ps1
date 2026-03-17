param(
    [string]$Python = "python",
    [string]$VenvPath = ".venv"
)

$ErrorActionPreference = "Stop"

& $Python -m venv $VenvPath

$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    throw "Python virtual environment was not created successfully: $PythonExe"
}

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -r .\requirements.txt

Write-Host ""
Write-Host "Environment ready."
Write-Host "Activate with:"
Write-Host "  .\\$VenvPath\\Scripts\\Activate.ps1"

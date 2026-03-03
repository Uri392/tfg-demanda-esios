# Ejecuta un script usando el Python del entorno virtual (.venv)
param(
    [Parameter(Mandatory=$true)]
    [string]$script
)

& ".\.venv\Scripts\python.exe" $script
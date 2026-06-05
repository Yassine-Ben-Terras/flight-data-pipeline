param([string]$CredentialsPath)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not (Test-Path '.env')) { Copy-Item '.env.example' '.env' }
New-Item -ItemType Directory -Force '.secrets' | Out-Null
if ($CredentialsPath) {
    $credentialObject = Get-Content -Raw -LiteralPath $CredentialsPath | ConvertFrom-Json
    if (-not $credentialObject.clientId -or -not $credentialObject.clientSecret) {
        throw 'Expected clientId and clientSecret in credential file'
    }
    Copy-Item -LiteralPath $CredentialsPath -Destination '.secrets/opensky.json'
}
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Docker startup failed' }
Write-Host 'Dashboard: http://localhost:3000 — see .env for login settings'

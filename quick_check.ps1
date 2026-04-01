Write-Host "=== QUICK SYSTEM CHECK ===" -ForegroundColor Cyan

# 1. Test NepseAPI directly
Write-Host "`n[1] Testing NepseAPI..."
try {
    $response = Invoke-WebRequest -Uri "https://nepseapi.surajrimal.dev/api/v1/market/live" -TimeoutSec 5
    if ($response.StatusCode -eq 200) {
        $data = $response.Content | ConvertFrom-Json
        Write-Host "[OK] NepseAPI: Connected ($(($data.data | Measure-Object).Count) stocks)"
    }
} catch {
    Write-Host "[ERROR] NepseAPI: Failed - $($_.Exception.Message)"
}

# 2. Test Flask
Write-Host "`n[2] Testing Flask Health..."
try {
    $health = curl http://localhost:5000/health -s | ConvertFrom-Json
    Write-Host "[OK] Status: $($health.status)"
    Write-Host "   Candles Loaded: $($health.candles_loaded)"
    Write-Host "   Data Fresh: $($health.data_fresh)"
    Write-Host "   Market Active: $($health.market_active)"
} catch {
    Write-Host "[ERROR] Flask not responding - Make sure main.py is running"
}

# 3. API Key check
Write-Host "`n[3] Environment Variables..."
$hasApiKey = -not [string]::IsNullOrEmpty($env:NEPSE_API_KEY)
$hasBotToken = -not [string]::IsNullOrEmpty($env:NEPSE_BOT_TOKEN)
$hasChatId = -not [string]::IsNullOrEmpty($env:NEPSE_CHAT_ID)

Write-Host "API_KEY: $(if ($hasApiKey) { '[OK] Set' } else { '[ERROR] NOT SET' })"
Write-Host "BOT_TOKEN: $(if ($hasBotToken) { '[OK] Set' } else { '[ERROR] NOT SET' })"
Write-Host "CHAT_ID: $(if ($hasChatId) { '[OK] Set' } else { '[ERROR] NOT SET' })"

Write-Host "`n=== END CHECK ===" -ForegroundColor Cyan
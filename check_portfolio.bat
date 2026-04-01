@echo off
REM ════════════════════════════════════════════════════════════════
REM Portfolio Health Check Script for Windows
REM ════════════════════════════════════════════════════════════════

setlocal enabledelayedexpansion

REM Get API key from .env file
for /f "tokens=2 delims==" %%a in ('findstr NEPSE_API_KEY .env') do set API_KEY=%%a

if "!API_KEY!"=="" (
    echo.
    echo [ERROR] NEPSE_API_KEY not found in .env file
    echo Please check your .env file and try again
    pause
    exit /b 1
)

echo.
echo ════════════════════════════════════════════════════════════════
echo                   PORTFOLIO HEALTH CHECK
echo ════════════════════════════════════════════════════════════════
echo.

REM Check system health first
echo [1/2] Checking system health...
echo.
curl -s http://localhost:5000/health > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Cannot connect to system
    echo Make sure program is running: python main.py
    echo.
    pause
    exit /b 1
)

REM Get and display portfolio
echo [2/2] Fetching portfolio data...
echo.

curl -s -H "X-API-Key: !API_KEY!" http://localhost:5000/portfolio_health | python -m json.tool

echo.
echo ════════════════════════════════════════════════════════════════
echo Portfolio check complete!
echo ════════════════════════════════════════════════════════════════
echo.

pause
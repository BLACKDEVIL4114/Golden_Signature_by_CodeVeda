@echo off
title AGPO - Always On Runner
cd /d "%~dp0"
echo ============================================
echo   AGPO Always-On Mode
echo ============================================
echo.
echo This will auto-restart Streamlit if it stops.
echo URL: http://localhost:8501
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\always_on_runner.ps1" -ProjectRoot "%~dp0" -Port 8501
pause

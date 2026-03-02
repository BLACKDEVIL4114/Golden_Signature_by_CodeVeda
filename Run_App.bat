@echo off
title AGPO - Manufacturing Dashboard
echo ============================================
echo   AGPO System Starting...
echo ============================================
echo.
cd /d "%~dp0"
echo Starting Streamlit app on http://localhost:8501
echo.
echo Keep this window open while using the app.
echo Close this window to stop the app.
echo ============================================
streamlit run app.py --server.port 8501
pause

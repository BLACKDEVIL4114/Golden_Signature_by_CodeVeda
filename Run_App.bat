@echo off
title AGPO - Manufacturing Dashboard
echo ============================================
echo   AGPO System Starting...
echo ============================================
echo.
cd /d "%~dp0"
set "VENV_PYTHON=%~dp0.venv\Scripts\python.exe"

if not exist "%VENV_PYTHON%" (
    echo Creating local virtual environment...
    python -m venv "%~dp0.venv"
    if errorlevel 1 (
        echo Failed to create virtual environment.
        echo Make sure Python is installed and available on PATH.
        pause
        exit /b 1
    )
)

echo Checking dependencies...
"%VENV_PYTHON%" -m pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo Installing project requirements...
    "%VENV_PYTHON%" -m pip install --upgrade pip
    "%VENV_PYTHON%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo Failed to install project requirements.
        pause
        exit /b 1
    )
)

echo Starting Streamlit app on http://localhost:8501
echo.
echo Keep this window open while using the app.
echo Close this window to stop the app.
echo ============================================
"%VENV_PYTHON%" -m streamlit run app.py --server.port 8501
pause

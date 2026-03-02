@echo off
echo ============================================
echo   Offer Letter Bot - Starting...
echo ============================================
echo.

:: Use the project's virtual environment Python directly
set "VENV_PYTHON=C:\Users\mier3\OneDrive\Desktop\PythonProject\DemoWebScraping\.venv\Scripts\python.exe"

:: Check if venv Python exists
if not exist "%VENV_PYTHON%" (
    echo [ERROR] Virtual environment not found at:
    echo   %VENV_PYTHON%
    echo.
    echo Please run: python -m venv .venv
    echo Then: .venv\Scripts\pip install flask google-auth google-auth-oauthlib google-api-python-client pyairtable
    pause
    exit /b 1
)

:: Open browser
start "" "http://localhost:5000"

:: Start the Flask app using venv Python
"%VENV_PYTHON%" "%~dp0app.py"

pause

@echo off
setlocal enabledelayedexpansion

cd /d "%~dp0"

where py >nul 2>&1
if %errorlevel% neq 0 (
  where python >nul 2>&1
  if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed.
    echo Install Python 3, then re-run this file.
    pause
    exit /b 1
  )
)

if not exist ".venv\Scripts\python.exe" (
  echo [setup] rebuilding .venv...
  if exist ".venv" rmdir /s /q ".venv"
  py -3 -m venv .venv 2>nul || python -m venv .venv
)

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] failed to create virtual environment.
  pause
  exit /b 1
)

echo [setup] installing/updating dependencies...
".venv\Scripts\python.exe" -m pip install -q --upgrade pip
".venv\Scripts\python.exe" -m pip install -q -r requirements.txt

if "%LOCAL_AGENT_PORT%"=="" set LOCAL_AGENT_PORT=8765
if "%LOCAL_AGENT_ALLOWED_ORIGINS%"=="" set LOCAL_AGENT_ALLOWED_ORIGINS=*
if "%CHROME_DEBUG_PORT%"=="" set CHROME_DEBUG_PORT=9223
if "%LAUNCH_CHROME_ON_START%"=="" set LAUNCH_CHROME_ON_START=1

if "%LAUNCH_CHROME_ON_START%"=="1" (
  echo [start] launching Chrome debug on port %CHROME_DEBUG_PORT%...
  start "" chrome --remote-debugging-port=%CHROME_DEBUG_PORT% --disable-features=BlockInsecurePrivateNetworkRequests --user-data-dir="%USERPROFILE%\\.chrome-debug-evidence"
)

echo [start] local agent =^> http://127.0.0.1:%LOCAL_AGENT_PORT%
echo [hint] keep this window open while using web deploy
".venv\Scripts\python.exe" -m uvicorn local_agent:app --host 127.0.0.1 --port %LOCAL_AGENT_PORT%

pause

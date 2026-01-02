@echo off
setlocal enabledelayedexpansion

echo === Searching for Anaconda/Miniconda python.exe (prefer D:) ===

set PYTHON_EXE=


for %%p in (
    "D:\anaconda3"
    "D:\anaconda"
    "D:\miniconda3"
    "%UserProfile%\anaconda3"
    "%UserProfile%\miniconda3"
) do (
    if exist %%~p\python.exe (
        set "PYTHON_EXE=%%~p\python.exe"
        goto FOUND
    )
)


if not defined PYTHON_EXE (
    echo Searching D:\ for python.exe containing anaconda/miniconda...
    for /f "delims=" %%f in ('
        where /r D:\ python.exe 2^>nul ^| findstr /i "anaconda miniconda"
    ') do (
        set "PYTHON_EXE=%%f"
        goto FOUND
    )
)

:FOUND
if not defined PYTHON_EXE (
    echo ERROR: Cannot find Anaconda/Miniconda python.exe!
    pause
    exit /b
)

echo Found python at: %PYTHON_EXE%


set "SCRIPT_PATH=%~dp0etl.py"

echo Running script: %SCRIPT_PATH%
"%PYTHON_EXE%" "%SCRIPT_PATH%"

echo Done.
pause

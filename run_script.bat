@echo off
REM Go to the directory containing this script itself.
cd /d "%~dp0"

REM Create a 'task_logs' subfolder if it doesn't already exist
if not exist task_logs mkdir task_logs

REM Delete logs older than 50 days
REM forfiles /p "task_logs" /s /m *.log /d -50 /c "cmd /c del @path"

REM Get date/time in a filename-safe format
set "datetime=%date:~-4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%"
REM Remove spaces from hour field (e.g. ' 2' -> '02')
set "datetime=%datetime: =0%"

REM Build log file path
set "logfile=task_logs\etl_%datetime%.log"

REM Run the script using uv
echo [%date% %time%] Running ETL job... >> "%logfile%"
echo ------------------------------------------- >> "%logfile%"
uv run main.py >> "%logfile%" 2>&1
REM powershell -Command "uv run main.py 2>&1 | Tee-Object -FilePath '%logfile%' -Append"
echo [%date% %time%] ETL job finished. >> "%logfile%"

REM Store the exit code of the script
set "exitcode=%errorlevel%"

REM Send message to Teams with status of the run
if %exitcode% neq 0 (
    echo [%date% %time%] ETL FAILED with exit code %exitcode%. >> "%logfile%"
    call uv run notify_teams.py "[%date% %time%] ETL FAILED" "The ETL job failed with exit code %exitcode%. Check logs: %cd%\%logfile%" "False"
    echo here >> "%logfile%"
) else (
    echo [%date% %time%] ETL job completed successfully. >> "%logfile%"
    call uv run notify_teams.py "[%date% %time%] ETL Succeeded" "The ETL job completed successfully." "True"
    echo here >> "%logfile%"
)
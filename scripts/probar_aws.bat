@echo off
rem Lanza en este PC el caso base contra la CENTRAL de AWS, cada módulo en su
rem ventana: cuatro estaciones (Monitor + Engine) y dos operarios. Los
rem operarios deben estar dados de alta en CENTRAL (docs/DESPLIEGUE_AWS.md).
rem Para cerrarlo todo: scripts\parar_local.bat
rem
rem   scripts\probar_aws.bat IP_AWS

if "%~1"=="" (
    echo Uso: scripts\probar_aws.bat IP_AWS
    exit /b 1
)
set IP=%~1

rem Todo se lanza desde la raíz del repositorio, esté donde esté quien lo llame.
cd /d "%~dp0.."

python -c "import socket; socket.create_connection(('%IP%', 9999), 3)" 2>nul
if errorlevel 1 (
    echo No se puede conectar con WM_Central en %IP%:9999. Revisa la IP y el grupo de seguridad.
    exit /b 1
)

start "WM WS-04 Monitor" cmd /k python WM_WS\WM_WS_M.py %IP% 9999 WS-04 "River Park" --puerto-engine 9100
start "WM WS-04 Engine" cmd /k python WM_WS\WM_WS_E.py %IP%:9092 localhost 9100
start "WM WS-05 Monitor" cmd /k python WM_WS\WM_WS_M.py %IP% 9999 WS-05 "Central Park" --puerto-engine 9101
start "WM WS-05 Engine" cmd /k python WM_WS\WM_WS_E.py %IP%:9092 localhost 9101
start "WM WS-07 Monitor" cmd /k python WM_WS\WM_WS_M.py %IP% 9999 WS-07 "Riverside Gardens" --puerto-engine 9102
start "WM WS-07 Engine" cmd /k python WM_WS\WM_WS_E.py %IP%:9092 localhost 9102
start "WM WS-11 Monitor" cmd /k python WM_WS\WM_WS_M.py %IP% 9999 WS-11 "North Roundabout" --puerto-engine 9103
start "WM WS-11 Engine" cmd /k python WM_WS\WM_WS_E.py %IP%:9092 localhost 9103

start "WM FO1" cmd /k python WM_FO\WM_FO.py %IP%:9092 FO1
start "WM FO2" cmd /k python WM_FO\WM_FO.py %IP%:9092 FO2

timeout /t 2 /nobreak >nul
start "" http://%IP%:8080

echo.
echo Listo. Para cerrar todas las ventanas: scripts\parar_local.bat

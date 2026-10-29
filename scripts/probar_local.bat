@echo off
rem Abre todo lo necesario para probar en este PC, cada módulo en su ventana:
rem Kafka (en WSL), CENTRAL, dos estaciones (Monitor + Engine), dos operarios
rem y el panel en el navegador. Para cerrarlo todo: scripts\parar_local.bat
rem
rem   scripts\probar_local.bat

rem Todo se lanza desde la raíz del repositorio, esté donde esté quien lo llame.
cd /d "%~dp0.."

rem Kafka: si ya escucha en el puerto 9092 no se vuelve a arrancar.
python -c "import socket; socket.create_connection(('localhost', 9092), 1)" 2>nul
if errorlevel 1 (
    echo Arrancando Kafka en WSL...
    start "WM Kafka" wsl.exe -e bash -lc "~/kafka.sh"
) else (
    echo Kafka ya estaba en marcha.
)

rem Operarios de prueba (si ya existen, solo se actualiza el nombre).
python scripts\gestionar_bd.py operario FO1 "Ana" >nul
python scripts\gestionar_bd.py operario FO2 "Luis" >nul

echo Esperando a que Kafka acepte conexiones...
:esperar_kafka
python -c "import socket; socket.create_connection(('localhost', 9092), 1)" 2>nul
if errorlevel 1 (
    timeout /t 1 /nobreak >nul
    goto esperar_kafka
)

start "WM Central" cmd /k python WM_Central\WM_Central.py 9999 localhost:9092
rem Un momento para que CENTRAL abra su puerto antes de que lleguen los Monitores.
timeout /t 3 /nobreak >nul

start "WM WS-04 Monitor" cmd /k python WM_WS\WM_WS_M.py localhost 9999 WS-04 "River Park" --puerto-engine 9100
start "WM WS-04 Engine" cmd /k python WM_WS\WM_WS_E.py localhost:9092 localhost 9100
start "WM WS-05 Monitor" cmd /k python WM_WS\WM_WS_M.py localhost 9999 WS-05 "Central Park" --puerto-engine 9101
start "WM WS-05 Engine" cmd /k python WM_WS\WM_WS_E.py localhost:9092 localhost 9101

start "WM FO1" cmd /k python WM_FO\WM_FO.py localhost:9092 FO1
start "WM FO2" cmd /k python WM_FO\WM_FO.py localhost:9092 FO2

timeout /t 2 /nobreak >nul
start "" http://localhost:8080

echo.
echo Listo. Para cerrar todas las ventanas: scripts\parar_local.bat

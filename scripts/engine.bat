@echo off
rem Lanza el Engine de una estación (WM_WS_E) en esta ventana. Debe ir en el
rem mismo PC que su Monitor, que es quien le da su ID.
rem
rem   scripts\engine.bat IP_KAFKA [PUERTO_ENGINE]
rem
rem   scripts\engine.bat 13.49.77.131 9100
rem
rem PUERTO_ENGINE debe coincidir con el que se dio al Monitor (9100 por defecto).
rem Con Docker Desktop en marcha se ejecuta en un contenedor; si no, con Python.
setlocal
if "%~1"=="" (
    echo Uso: scripts\engine.bat IP_KAFKA [PUERTO_ENGINE]
    exit /b 1
)
set "PUERTO_ENGINE=%~2"
if "%PUERTO_ENGINE%"=="" set "PUERTO_ENGINE=9100"
if not defined WM_IMAGEN set "WM_IMAGEN=antoniogg03/practica-aws:latest"

cd /d "%~dp0.."
title WM Engine %PUERTO_ENGINE%

docker info >nul 2>&1
if errorlevel 1 (
    echo Docker no disponible: se ejecuta con Python
    python -c "import kafka" 2>nul
    if errorlevel 1 (
        echo Falta kafka-python. Instalalo con: python -m pip install --user -r requirements.txt
        exit /b 1
    )
    python WM_WS\WM_WS_E.py %~1:9092 localhost %PUERTO_ENGINE%
    exit /b
)
rem El Monitor está en la red wm-red con el nombre wm-monitor-PUERTO_ENGINE.
rem El volumen conserva el riego en curso: si el Engine cae y se relanza, envía
rem a CENTRAL lo que llegó a regar.
docker network inspect wm-red >nul 2>&1 || docker network create wm-red >nul
docker run --rm -it --name wm-engine-%PUERTO_ENGINE% --network wm-red ^
    -v wm-engine:/app/data %WM_IMAGEN% ^
    python WM_WS/WM_WS_E.py %~1:9092 wm-monitor-%PUERTO_ENGINE% %PUERTO_ENGINE%

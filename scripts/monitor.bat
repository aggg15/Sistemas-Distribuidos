@echo off
rem Lanza el Monitor de una estación (WM_WS_M) en esta ventana.
rem
rem   scripts\monitor.bat IP_CENTRAL WS_ID "UBICACION" [PUERTO_ENGINE]
rem
rem   scripts\monitor.bat 13.49.77.131 WS-04 "River Park" 9100
rem
rem PUERTO_ENGINE (9100 por defecto) es donde el Monitor espera a su Engine:
rem cada estación del mismo PC necesita uno distinto (9100, 9101...).
rem
rem Con Docker Desktop en marcha se ejecuta en un contenedor con la imagen que
rem publica GitHub Actions (WM_IMAGEN la cambia); si no, con Python.
setlocal
if "%~3"=="" (
    echo Uso: scripts\monitor.bat IP_CENTRAL WS_ID "UBICACION" [PUERTO_ENGINE]
    exit /b 1
)
set "PUERTO_ENGINE=%~4"
if "%PUERTO_ENGINE%"=="" set "PUERTO_ENGINE=9100"
if not defined WM_IMAGEN set "WM_IMAGEN=antoniogg03/practica-aws:latest"

rem Todo se lanza desde la raíz del repositorio, esté donde esté quien lo llame.
cd /d "%~dp0.."
title WM %~2 Monitor

docker info >nul 2>&1
if errorlevel 1 (
    echo Docker no disponible: se ejecuta con Python
    python WM_WS\WM_WS_M.py %~1 9999 %~2 "%~3" --puerto-engine %PUERTO_ENGINE%
    exit /b
)
rem En Windows los contenedores no comparten localhost con el PC: Monitor y
rem Engine se encuentran en la red wm-red, el Monitor con el nombre
rem wm-monitor-PUERTO_ENGINE.
docker network inspect wm-red >nul 2>&1 || docker network create wm-red >nul
docker run --rm -it --name wm-monitor-%~2 --network wm-red ^
    --network-alias wm-monitor-%PUERTO_ENGINE% %WM_IMAGEN% ^
    python WM_WS/WM_WS_M.py %~1 9999 %~2 "%~3" --puerto-engine %PUERTO_ENGINE%

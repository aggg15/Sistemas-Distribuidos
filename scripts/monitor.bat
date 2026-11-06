@echo off
rem Lanza el Monitor de una estación (WM_WS_M) en esta ventana.
rem
rem   scripts\monitor.bat IP_CENTRAL WS_ID "UBICACION" [PUERTO_ENGINE]
rem
rem   scripts\monitor.bat 13.49.77.131 WS-04 "River Park" 9100
rem
rem PUERTO_ENGINE (9100 por defecto) es donde el Monitor espera a su Engine:
rem cada estación del mismo PC necesita uno distinto (9100, 9101...).
setlocal
if "%~3"=="" (
    echo Uso: scripts\monitor.bat IP_CENTRAL WS_ID "UBICACION" [PUERTO_ENGINE]
    exit /b 1
)
set "PUERTO_ENGINE=%~4"
if "%PUERTO_ENGINE%"=="" set "PUERTO_ENGINE=9100"

rem Todo se lanza desde la raíz del repositorio, esté donde esté quien lo llame.
cd /d "%~dp0.."
title WM %~2 Monitor
python WM_WS\WM_WS_M.py %~1 9999 %~2 "%~3" --puerto-engine %PUERTO_ENGINE%

@echo off
rem Lanza el Engine de una estación (WM_WS_E) en esta ventana. Debe ir en el
rem mismo PC que su Monitor: se conecta a él por localhost y recibe de él su ID.
rem
rem   scripts\engine.bat IP_KAFKA [PUERTO_ENGINE]
rem
rem   scripts\engine.bat 13.49.77.131 9100
rem
rem PUERTO_ENGINE debe coincidir con el que se dio al Monitor (9100 por defecto).
setlocal
if "%~1"=="" (
    echo Uso: scripts\engine.bat IP_KAFKA [PUERTO_ENGINE]
    exit /b 1
)
set "PUERTO_ENGINE=%~2"
if "%PUERTO_ENGINE%"=="" set "PUERTO_ENGINE=9100"

cd /d "%~dp0.."
python -c "import kafka" 2>nul
if errorlevel 1 (
    echo Falta kafka-python. Instalalo con: python -m pip install --user -r requirements.txt
    exit /b 1
)
title WM Engine %PUERTO_ENGINE%
python WM_WS\WM_WS_E.py %~1:9092 localhost %PUERTO_ENGINE%

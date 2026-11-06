@echo off
rem Lanza WM_Central en esta ventana, para pruebas en este PC. En la corrección
rem CENTRAL corre en AWS (scripts/central.sh en la instancia EC2).
rem
rem   scripts\central.bat [KAFKA] [PUERTO_SOCKETS]
rem
rem   scripts\central.bat                       Kafka en localhost:9092, sockets en 9999
rem   scripts\central.bat localhost:9092 9999
rem
rem El panel queda en http://localhost:8080. Los operarios se dan de alta con
rem   python scripts\gestionar_bd.py operario FO1 "Ana"
setlocal
set "KAFKA=%~1"
if "%KAFKA%"=="" set "KAFKA=localhost:9092"
set "PUERTO=%~2"
if "%PUERTO%"=="" set "PUERTO=9999"

cd /d "%~dp0.."
python -c "import kafka" 2>nul
if errorlevel 1 (
    echo Falta kafka-python. Instalalo con: python -m pip install --user -r requirements.txt
    exit /b 1
)
title WM Central
python WM_Central\WM_Central.py %PUERTO% %KAFKA%

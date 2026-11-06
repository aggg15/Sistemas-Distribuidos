@echo off
rem Lanza un operario (WM_FO) en esta ventana. Debe estar dado de alta en CENTRAL.
rem
rem   scripts\operario.bat IP_KAFKA FO_ID [FICHERO_PETICIONES]
rem
rem   scripts\operario.bat 13.49.77.131 FO1
rem   scripts\operario.bat 13.49.77.131 FO2 WM_FO\peticiones_ejemplo.txt
setlocal
if "%~2"=="" (
    echo Uso: scripts\operario.bat IP_KAFKA FO_ID [FICHERO_PETICIONES]
    exit /b 1
)
rem La ruta del fichero se resuelve antes de cambiar de carpeta.
set "FICHERO="
if not "%~3"=="" (
    if not exist "%~f3" (
        echo No existe el fichero de peticiones: %~3
        exit /b 1
    )
    set "FICHERO=%~f3"
)

cd /d "%~dp0.."
python -c "import kafka" 2>nul
if errorlevel 1 (
    echo Falta kafka-python. Instalalo con: python -m pip install --user -r requirements.txt
    exit /b 1
)
title WM %~2
if defined FICHERO (
    python WM_FO\WM_FO.py %~1:9092 %~2 --fichero "%FICHERO%"
) else (
    python WM_FO\WM_FO.py %~1:9092 %~2
)

@echo off
rem Lanza un operario (WM_FO) en esta ventana. Debe estar dado de alta en CENTRAL.
rem
rem   scripts\operario.bat IP_KAFKA FO_ID [FICHERO_PETICIONES]
rem
rem   scripts\operario.bat 13.49.77.131 FO1
rem   scripts\operario.bat 13.49.77.131 FO2 WM_FO\peticiones_5_riegos.txt
rem
rem Con Docker Desktop en marcha se ejecuta en un contenedor; si no, con Python.
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
if not defined WM_IMAGEN set "WM_IMAGEN=antoniogg03/practica-aws:latest"

cd /d "%~dp0.."
title WM %~2

docker info >nul 2>&1
if errorlevel 1 goto python

rem El contenedor no ve los ficheros del PC: el de peticiones se monta en
rem solo lectura.
if defined FICHERO (
    docker run --rm -it --name wm-fo-%~2 -v "%FICHERO%:/tmp/peticiones.txt:ro" ^
        %WM_IMAGEN% python WM_FO/WM_FO.py %~1:9092 %~2 --fichero /tmp/peticiones.txt
) else (
    docker run --rm -it --name wm-fo-%~2 %WM_IMAGEN% python WM_FO/WM_FO.py %~1:9092 %~2
)
exit /b

:python
echo Docker no disponible: se ejecuta con Python
python -c "import kafka" 2>nul
if errorlevel 1 (
    echo Falta kafka-python. Instalalo con: python -m pip install --user -r requirements.txt
    exit /b 1
)
if defined FICHERO (
    python WM_FO\WM_FO.py %~1:9092 %~2 --fichero "%FICHERO%"
) else (
    python WM_FO\WM_FO.py %~1:9092 %~2
)

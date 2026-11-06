@echo off
rem Cierra las ventanas que abrieron scripts\probar_local.bat o
rem scripts\probar_aws.bat y los programas que corren dentro de ellas.
rem
rem   scripts\parar_local.bat           todo menos Kafka
rem   scripts\parar_local.bat kafka     también Kafka

rem "WM Engine" cubre las ventanas de scripts\engine.bat ("WM Engine 9100"...).
for %%v in ("WM Central" "WM WS-04 Monitor" "WM WS-04 Engine" "WM WS-05 Monitor" "WM WS-05 Engine" "WM WS-07 Monitor" "WM WS-07 Engine" "WM WS-11 Monitor" "WM WS-11 Engine" "WM Engine" "WM FO1" "WM FO2") do (
    taskkill /fi "WINDOWTITLE eq %%~v*" /t /f >nul 2>&1
)

rem Cerrar la ventana no siempre para su contenedor: se paran los que lanzaron
rem monitor.bat, engine.bat y operario.bat (todos se llaman wm-...).
for /f %%c in ('docker ps -q --filter "name=wm-" 2^>nul') do docker rm -f %%c >nul 2>&1

if /i "%~1"=="kafka" (
    rem Se para de forma ordenada; en WSL 1 matarlo de golpe estropea sus datos.
    wsl.exe -e bash -lc "~/kafka_2.13-3.9.1/bin/kafka-server-stop.sh" >nul 2>&1
    timeout /t 5 /nobreak >nul
    taskkill /fi "WINDOWTITLE eq WM Kafka*" /t /f >nul 2>&1
)

echo Ventanas de prueba cerradas.

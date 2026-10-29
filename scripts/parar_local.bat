@echo off
rem Cierra las ventanas que abrió scripts\probar_local.bat (su título empieza
rem por "WM ") y los programas que corren dentro de ellas.
rem
rem   scripts\parar_local.bat           todo menos Kafka
rem   scripts\parar_local.bat kafka     también Kafka

for %%v in ("WM Central" "WM WS-04 Monitor" "WM WS-04 Engine" "WM WS-05 Monitor" "WM WS-05 Engine" "WM FO1" "WM FO2") do (
    taskkill /fi "WINDOWTITLE eq %%~v*" /t /f >nul 2>&1
)

if /i "%~1"=="kafka" (
    rem Se para de forma ordenada; en WSL 1 matarlo de golpe estropea sus datos.
    wsl.exe -e bash -lc "~/kafka_2.13-3.9.1/bin/kafka-server-stop.sh" >nul 2>&1
    timeout /t 5 /nobreak >nul
    taskkill /fi "WINDOWTITLE eq WM Kafka*" /t /f >nul 2>&1
)

echo Ventanas de prueba cerradas.

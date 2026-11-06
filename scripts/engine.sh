#!/usr/bin/env bash
# Lanza el Engine de una estación (WM_WS_E) en este PC. Debe ir en el mismo PC
# que su Monitor: se conecta a él por localhost y recibe de él su ID.
#
#   scripts/engine.sh IP_KAFKA [PUERTO_ENGINE]
#
#   scripts/engine.sh 13.49.77.131 9100
#
# PUERTO_ENGINE debe coincidir con el que se dio al Monitor (9100 por defecto).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -lt 1 ]; then
    echo "Uso: scripts/engine.sh IP_KAFKA [PUERTO_ENGINE]" >&2
    exit 1
fi
IP=$1
PUERTO_ENGINE=${2:-9100}

. scripts/lanzar.sh
# El volumen conserva el riego en curso (data/engine_<WS>.json): si el Engine
# cae y se relanza, envía a CENTRAL lo que llegó a regar.
lanzar "wm-engine-$PUERTO_ENGINE" "-v wm-engine:/app/data" \
    WM_WS/WM_WS_E.py "$IP:9092" localhost "$PUERTO_ENGINE"

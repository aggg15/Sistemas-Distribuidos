#!/usr/bin/env bash
# Lanza el Monitor de una estación (WM_WS_M) en este PC.
#
#   scripts/monitor.sh IP_CENTRAL WS_ID "UBICACION" [PUERTO_ENGINE]
#
#   scripts/monitor.sh 13.49.77.131 WS-04 "River Park" 9100
#
# PUERTO_ENGINE (9100 por defecto) es donde el Monitor espera a su Engine:
# cada estación del mismo PC necesita uno distinto (9100, 9101...).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -lt 3 ]; then
    echo "Uso: scripts/monitor.sh IP_CENTRAL WS_ID \"UBICACION\" [PUERTO_ENGINE]" >&2
    exit 1
fi
IP=$1
WS_ID=$2
UBICACION=$3
PUERTO_ENGINE=${4:-9100}

. scripts/lanzar.sh
lanzar "wm-monitor-$WS_ID" "" \
    WM_WS/WM_WS_M.py "$IP" 9999 "$WS_ID" "$UBICACION" --puerto-engine "$PUERTO_ENGINE"

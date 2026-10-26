"""Altas de operarios y consulta de los datos persistentes de CENTRAL."""

import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.base_datos import BaseDatos, RUTA_BD  # noqa: E402
from common.fechas import fecha_madrid  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("WM_DB_PATH", RUTA_BD))
    acciones = parser.add_subparsers(dest="accion", required=True)
    alta = acciones.add_parser("operario", help="Crear o actualizar un operario")
    alta.add_argument("id")
    alta.add_argument("nombre")
    for accion in ("operarios", "estaciones", "riegos"):
        acciones.add_parser(accion, help=f"Listar {accion} guardados")
    args = parser.parse_args(argv)
    try:
        bd = BaseDatos(args.db)
        if args.accion == "operario":
            bd.guardar_operario(args.id, args.nombre)
            print(f"Operario guardado: {args.id}")
        else:
            listar = {"operarios": bd.listar_operarios,
                      "estaciones": bd.listar_estaciones, "riegos": bd.listar_riegos}
            datos = listar[args.accion]()
            for fila in datos:
                for campo in ("ultima_conexion", "inicio", "fin"):
                    if fila.get(campo) is not None:
                        fila[campo] = fecha_madrid(fila[campo])
            print(json.dumps(datos, ensure_ascii=False, indent=2))
    except (OSError, sqlite3.Error, ValueError) as error:
        parser.exit(1, f"Error de base de datos: {error}\n")


if __name__ == "__main__":
    main()

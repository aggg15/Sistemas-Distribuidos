"""Genera la CA del proyecto y el certificado TLS de CENTRAL y WM_Registry.

    python scripts/generar_certificados.py 3.80.1.2
    python scripts/generar_certificados.py 3.80.1.2 wm.ejemplo.com --carpeta certs
    python scripts/generar_certificados.py 3.80.1.2 --nueva-ca

Se ejecuta en la máquina del servidor (AWS). Indica la IP pública (mejor una
Elastic IP) y, si hay, el nombre DNS: los clientes comprueban que coinciden.
localhost y 127.0.0.1 se añaden siempre para las pruebas locales.

Después, copia SOLO certs/ca.pem a los PCs del laboratorio. Las claves .key no
salen nunca del servidor ni se suben a Git.
"""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.certificados import NOMBRES_LOCALES, generar_en  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("nombres", nargs="*", help="IPs o nombres DNS del servidor")
    parser.add_argument("--carpeta", default="certs")
    parser.add_argument("--nueva-ca", action="store_true",
                        help="Crear una CA nueva aunque exista (habrá que volver a copiar ca.pem)")
    args = parser.parse_args(argv)
    try:
        rutas = generar_en(args.carpeta, [*args.nombres, *NOMBRES_LOCALES], args.nueva_ca)
    except (OSError, ValueError) as error:
        parser.exit(1, f"No se pudieron generar los certificados: {error}\n")
    print(f"Certificado del servidor: {rutas['cert']} (válido para "
          f"{', '.join([*args.nombres, *NOMBRES_LOCALES])})")
    print(f"Clave del servidor:       {rutas['key']}  (privada, no compartir)")
    print(f"CA para los clientes:     {rutas['ca']}  (copiar a los PCs del laboratorio)")
    print(f"Clave de la CA:           {rutas['ca_key']}  (privada, no compartir)")


if __name__ == "__main__":
    main()

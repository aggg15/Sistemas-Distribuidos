"""Monitor del anexo: registra una estación y muestra la respuesta."""

import argparse
import socket

from protocolo import enviar_mensaje, recibir_mensaje


def main():
    parser = argparse.ArgumentParser(description="Monitor de una estación de riego")
    parser.add_argument("host", help="Dirección de la central, por ejemplo localhost")
    parser.add_argument("puerto", type=int)
    parser.add_argument("id_estacion")
    parser.add_argument("ubicacion", help="Usa comillas si contiene espacios")
    args = parser.parse_args()
    if not 1 <= args.puerto <= 65535:
        parser.error("El puerto debe estar entre 1 y 65535")
    if "#" in args.id_estacion or "#" in args.ubicacion:
        parser.error("El ID y la ubicación no pueden contener #")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as conexion:
            conexion.connect((args.host, args.puerto))
            mensaje = f"REGISTRO#{args.id_estacion}#{args.ubicacion}"
            print(f"Enviando: {mensaje}")
            enviar_mensaje(conexion, mensaje)

            respuesta = recibir_mensaje(conexion)
            print(f"Respuesta de la central: {respuesta}")
    except (OSError, ValueError) as error:
        parser.exit(1, f"Error de comunicación con la central: {error}\n")


if __name__ == "__main__":
    main()

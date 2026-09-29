"""Central del anexo: un hilo por estación, usando socket y threading."""

import argparse
import socket
import threading

from protocolo import enviar_mensaje, recibir_mensaje


def procesar_registro(mensaje):
    # Python conserva el último campo vacío en "REGISTRO#WS-04#".split("#").
    campos = mensaje.split("#")
    if len(campos) != 3:
        return "STATUS#ERROR#Se esperan tres campos: REGISTRO, ID y UBICACION"
    if campos[0] != "REGISTRO":
        return "STATUS#ERROR#Operacion desconocida"

    id_estacion = campos[1].strip()
    ubicacion = campos[2].strip()
    if not id_estacion or not ubicacion:
        return "STATUS#ERROR#El ID y la ubicacion son obligatorios"

    # El anexo pide validar y mostrar. Añadiremos persistencia más adelante.
    print(f"Estación registrada: {id_estacion} | Ubicación: {ubicacion}")
    return "STATUS#OK#Estacion registrada correctamente"


def atender_estacion(conexion, direccion):
    # with cierra este socket al terminar, también si se produce una excepción.
    with conexion:
        try:
            mensaje = recibir_mensaje(conexion)
            print(f"Recibido de {direccion}: {mensaje}")
            respuesta = procesar_registro(mensaje)
            enviar_mensaje(conexion, respuesta)
        except (OSError, ValueError) as error:
            print(f"Error con la estación {direccion}: {error}")


def main():
    parser = argparse.ArgumentParser(description="Central de registro de estaciones")
    parser.add_argument("puerto", type=int, help="Puerto de escucha, por ejemplo 9999")
    args = parser.parse_args()
    if not 1 <= args.puerto <= 65535:
        parser.error("El puerto debe estar entre 1 y 65535")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as servidor:
            servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            servidor.bind(("0.0.0.0", args.puerto))
            servidor.listen()
            print(f"WM_Central escuchando en el puerto {args.puerto}", flush=True)

            while True:
                conexion, direccion = servidor.accept()
                print(f"Nueva conexión: {direccion}")
                hilo = threading.Thread(
                    target=atender_estacion,
                    args=(conexion, direccion),
                    daemon=True,
                )
                hilo.start()
    except KeyboardInterrupt:
        # Los hilos daemon no mantienen vivo el programa al detener la central.
        print("\nCentral detenida.")
    except OSError as error:
        parser.exit(1, f"Error en la central: {error}\n")


if __name__ == "__main__":
    main()

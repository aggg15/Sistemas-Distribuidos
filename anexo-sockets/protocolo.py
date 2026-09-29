"""Mensajes UTF-8 precedidos por una cabecera de longitud de 64 bytes."""

HEADER = 64
FORMAT = "utf-8"
MAX_MENSAJE = 4096


def enviar_mensaje(conexion, texto):
    datos = texto.encode(FORMAT)
    if not 0 < len(datos) <= MAX_MENSAJE:
        raise ValueError("El mensaje debe ocupar entre 1 y 4096 bytes")

    # Igual que en el ejemplo de clase: longitud decimal rellenada con espacios.
    cabecera = str(len(datos)).encode("ascii").ljust(HEADER, b" ")
    conexion.sendall(cabecera + datos)


def recibir_exactamente(conexion, cantidad):
    """TCP puede entregar un mensaje en partes: leemos hasta reunirlas todas."""
    datos = bytearray()
    while len(datos) < cantidad:
        parte = conexion.recv(cantidad - len(datos))
        if not parte:
            raise ConnectionError("La conexión se cerró antes de completar el mensaje")
        datos.extend(parte)
    return bytes(datos)


def recibir_mensaje(conexion):
    cabecera = recibir_exactamente(conexion, HEADER)
    longitud_texto = cabecera.decode("ascii").strip()
    if not longitud_texto.isdecimal():
        raise ValueError("La cabecera debe contener una longitud decimal")
    longitud = int(longitud_texto)
    if not 0 < longitud <= MAX_MENSAJE:
        raise ValueError("La longitud debe estar entre 1 y 4096 bytes")

    datos = recibir_exactamente(conexion, longitud)
    return datos.decode(FORMAT)

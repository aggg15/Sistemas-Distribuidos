"""Protocolo de sockets <STX><DATA><ETX><LRC> con ACK/NACK, ENQ y EOT.

Flujo de una conexión (página 12 del enunciado):

    cliente ── ENQ ──▶ servidor          saludo inicial
    cliente ◀── ACK ── servidor
    emisor  ── <STX>PETICION<ETX><LRC> ──▶ receptor ─┐
    emisor  ◀── ACK/NACK ──────────────── receptor   │ se repite mientras
    emisor  ◀── <STX>RESPUESTA<ETX><LRC> ─ receptor   │ la conexión esté abierta
    emisor  ── ACK/NACK ──────────────────▶ receptor ─┘
    cualquiera ── EOT ──▶                cierre ordenado
"""

STX = b"\x02"
ETX = b"\x03"
EOT = b"\x04"
ENQ = b"\x05"
ACK = b"\x06"
NACK = b"\x15"

FORMAT = "utf-8"
MAX_DATOS = 4096
MAX_INTENTOS = 3


class ErrorProtocolo(Exception):
    """El otro extremo no respeta el protocolo o se agotaron los reintentos."""


class ConexionCerrada(Exception):
    """El otro extremo envió EOT: cierre ordenado, no es un fallo."""


def calcular_lrc(datos):
    # XOR byte a byte de DATA (sin STX ni ETX), como indica XOR(MESSAGE).
    lrc = 0
    for byte in datos:
        lrc ^= byte
    return lrc


def empaquetar(texto):
    datos = texto.encode(FORMAT)
    if not 0 < len(datos) <= MAX_DATOS:
        raise ValueError(f"El mensaje debe ocupar entre 1 y {MAX_DATOS} bytes")
    # En UTF-8 los bytes 0x02 y 0x03 solo aparecen si el texto los contiene.
    if STX in datos or ETX in datos:
        raise ValueError("El mensaje no puede contener los caracteres STX o ETX")
    return STX + datos + ETX + bytes([calcular_lrc(datos)])


class Canal:
    """Envuelve un socket TCP conectado y habla el protocolo sobre él.

    Un canal solo debe usarse desde un hilo a la vez: un ciclo
    petición/respuesta no puede mezclarse con otro en el mismo socket.
    """

    def __init__(self, conexion):
        self.conexion = conexion

    # --- Lectura y escritura de bytes sueltos -------------------------------

    def _leer_byte(self):
        byte = self.conexion.recv(1)
        if not byte:
            # recv() devuelve b"" cuando el otro extremo cierra sin enviar EOT.
            raise ConnectionError("La conexión se cerró sin EOT")
        return byte

    def _leer_control(self):
        """Espera ACK o NACK. Devuelve True si es ACK."""
        byte = self._leer_byte()
        if byte == ACK:
            return True
        if byte == NACK:
            return False
        if byte == EOT:
            raise ConexionCerrada()
        raise ErrorProtocolo(f"Se esperaba ACK o NACK y llegó {byte!r}")

    # --- Saludo y cierre ----------------------------------------------------

    def saludar(self):
        """Lado cliente: envía ENQ y espera ACK."""
        self.conexion.sendall(ENQ)
        if not self._leer_control():
            raise ErrorProtocolo("El servidor rechazó la conexión (NACK)")

    def esperar_saludo(self, aceptar=True):
        """Lado servidor: espera ENQ y responde ACK o, si no acepta, NACK."""
        byte = self._leer_byte()
        if byte != ENQ:
            raise ErrorProtocolo(f"Se esperaba ENQ y llegó {byte!r}")
        self.conexion.sendall(ACK if aceptar else NACK)

    def cerrar(self):
        """Envía EOT para indicar un cierre ordenado. El socket lo cierra quien lo creó."""
        try:
            self.conexion.sendall(EOT)
        except OSError:
            pass  # Si el otro extremo ya no está, no hay a quién avisar.

    # --- Tramas -------------------------------------------------------------

    def enviar_trama(self, texto):
        """Envía una trama y espera ACK; ante NACK la reenvía hasta MAX_INTENTOS veces."""
        trama = empaquetar(texto)
        for _ in range(MAX_INTENTOS):
            self.conexion.sendall(trama)
            if self._leer_control():
                return
        raise ErrorProtocolo(f"Trama rechazada {MAX_INTENTOS} veces")

    def recibir_trama(self):
        """Recibe una trama, comprueba el LRC y responde ACK o NACK."""
        for _ in range(MAX_INTENTOS):
            datos, lrc = self._leer_trama()
            if calcular_lrc(datos) == lrc:
                self.conexion.sendall(ACK)
                try:
                    return datos.decode(FORMAT)
                except UnicodeDecodeError as error:
                    raise ErrorProtocolo("La trama no es texto UTF-8 válido") from error
            self.conexion.sendall(NACK)
        raise ErrorProtocolo(f"LRC incorrecto {MAX_INTENTOS} veces")

    def _leer_trama(self):
        inicio = self._leer_byte()
        if inicio == EOT:
            raise ConexionCerrada()
        if inicio != STX:
            raise ErrorProtocolo(f"Se esperaba STX y llegó {inicio!r}")

        datos = bytearray()
        while True:
            byte = self._leer_byte()
            if byte == ETX:
                break
            datos.extend(byte)
            if len(datos) > MAX_DATOS:
                raise ErrorProtocolo(f"La trama supera {MAX_DATOS} bytes sin ETX")

        # El LRC es siempre el byte siguiente a ETX, valga lo que valga.
        lrc = self._leer_byte()[0]
        return bytes(datos), lrc

    # --- Ciclo completo petición/respuesta ----------------------------------

    def peticion(self, texto):
        """Envía una petición y devuelve la respuesta del otro extremo."""
        self.enviar_trama(texto)
        return self.recibir_trama()

    def responder(self, procesar):
        """Recibe una petición, la pasa a procesar() y envía lo que devuelva."""
        peticion = self.recibir_trama()
        respuesta = procesar(peticion)
        self.enviar_trama(respuesta)
        return peticion

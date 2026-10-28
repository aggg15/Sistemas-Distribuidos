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

from contextlib import contextmanager
import math
import socket
import time

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

    def __init__(self, conexion, timeout=3.0):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("El tiempo de espera debe ser positivo y finito")
        self.conexion = conexion
        self.timeout = timeout
        self._limite = None

    @contextmanager
    def _operacion(self):
        """Un plazo total evita que un emisor lento retenga el canal byte a byte."""
        anterior = self._limite
        timeout_anterior = self.conexion.gettimeout()
        limite = time.monotonic() + self.timeout
        self._limite = min(anterior, limite) if anterior is not None else limite
        try:
            yield
        finally:
            self._limite = anterior
            try:
                self.conexion.settimeout(timeout_anterior)
            except OSError:
                pass  # El propietario puede haber cerrado ya el socket.

    def _preparar_io(self):
        restante = self._limite - time.monotonic()
        if restante <= 0:
            raise TimeoutError("Se agotó el tiempo de la operación de protocolo")
        self.conexion.settimeout(restante)

    def _enviar(self, datos):
        self._preparar_io()
        try:
            self.conexion.sendall(datos)
        except socket.timeout as error:
            raise TimeoutError("Se agotó el tiempo enviando datos") from error

    # --- Lectura y escritura de bytes sueltos -------------------------------

    def _leer_byte(self):
        self._preparar_io()
        try:
            byte = self.conexion.recv(1)
        except socket.timeout as error:
            # En Python 3.9 socket.timeout no es TimeoutError (lo es desde 3.10).
            # Se traduce para que quien use el canal capture siempre TimeoutError.
            raise TimeoutError("Se agotó el tiempo esperando datos") from error
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
        with self._operacion():
            self._enviar(ENQ)
            if not self._leer_control():
                raise ErrorProtocolo("El servidor rechazó la conexión (NACK)")

    def esperar_saludo(self, aceptar=True):
        """Lado servidor: espera ENQ y responde ACK o, si no acepta, NACK."""
        with self._operacion():
            byte = self._leer_byte()
            if byte != ENQ:
                raise ErrorProtocolo(f"Se esperaba ENQ y llegó {byte!r}")
            self._enviar(ACK if aceptar else NACK)

    def cerrar(self):
        """Envía EOT para indicar un cierre ordenado. El socket lo cierra quien lo creó."""
        try:
            with self._operacion():
                self._enviar(EOT)
        except OSError:
            pass  # Si el otro extremo ya no está, no hay a quién avisar.

    # --- Tramas -------------------------------------------------------------

    def enviar_trama(self, texto):
        """Envía una trama y espera ACK; ante NACK la reenvía hasta MAX_INTENTOS veces."""
        trama = empaquetar(texto)
        with self._operacion():
            for _ in range(MAX_INTENTOS):
                self._enviar(trama)
                if self._leer_control():
                    return
        raise ErrorProtocolo(f"Trama rechazada {MAX_INTENTOS} veces")

    def recibir_trama(self):
        """Confirma únicamente tramas con LRC, tamaño y texto UTF-8 válidos."""
        with self._operacion():
            for _ in range(MAX_INTENTOS):
                datos, lrc = self._leer_trama()
                texto = None
                try:
                    if datos and STX not in datos and calcular_lrc(datos) == lrc:
                        texto = datos.decode(FORMAT)
                except UnicodeDecodeError:
                    pass
                if texto is not None:
                    self._enviar(ACK)
                    return texto
                self._enviar(NACK)
        raise ErrorProtocolo(f"Trama inválida {MAX_INTENTOS} veces")

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
        with self._operacion():
            self.enviar_trama(texto)
            return self.recibir_trama()

    def responder(self, procesar):
        """Recibe una petición, la pasa a procesar() y envía lo que devuelva."""
        with self._operacion():
            peticion = self.recibir_trama()
            respuesta = procesar(peticion)
            self.enviar_trama(respuesta)
            return peticion

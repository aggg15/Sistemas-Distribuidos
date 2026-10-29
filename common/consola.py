"""Salida por terminal de todos los módulos.

- Mensajes cortos: hora y texto, con color según su importancia.
- Los menús no se rompen: si llega un mensaje mientras se espera una opción,
  se escribe encima y la pregunta vuelve a aparecer debajo.

Todo lo que se muestra pasa por escribir(), protegido con un lock, porque los
mensajes llegan desde varios hilos (Kafka, sockets, riego) a la vez.
"""

import logging
import os
import sys
import threading

# Códigos ANSI. La consola de Windows 10 los entiende si se activan (_activar_color).
COLORES = {"verde": "32", "rojo": "31", "amarillo": "33", "cian": "36", "gris": "90",
           "negrita": "1"}
COLOR_NIVEL = {logging.DEBUG: "gris", logging.WARNING: "amarillo",
               logging.ERROR: "rojo", logging.CRITICAL: "rojo"}
NOMBRE_NIVEL = {logging.WARNING: "AVISO", logging.ERROR: "ERROR",
                logging.CRITICAL: "ERROR"}

_lock = threading.Lock()
_pregunta = None  # texto de la pregunta mientras se espera lo que escribe el usuario
_color = False


def _activar_color(flujo):
    """True si se pueden usar colores: solo en una terminal, no en un fichero."""
    if not flujo.isatty():
        return False
    if os.name != "nt":
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        salida = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        modo = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(salida, ctypes.byref(modo)):
            return False
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING: interpretar los códigos ANSI.
        return bool(kernel32.SetConsoleMode(salida, modo.value | 0x0004))
    except (AttributeError, OSError):
        return False


def pintar(texto, color):
    if not _color or color is None:
        return texto
    return f"\x1b[{COLORES[color]}m{texto}\x1b[0m"


def escribir(texto="", color=None):
    """Escribe una o varias líneas sin romper la pregunta que esté en pantalla."""
    texto = pintar(texto, color)
    with _lock:
        if _pregunta is None:
            sys.stdout.write(texto + "\n")
        else:
            # Con color se borra la línea de la pregunta (\r y \x1b[2K); sin él
            # (salida a un fichero) basta con saltar de línea.
            inicio = "\r\x1b[2K" if _color else "\n"
            sys.stdout.write(f"{inicio}{texto}\n{_pregunta}")
        sys.stdout.flush()


def preguntar(texto):
    """input() que los mensajes de otros hilos no tapan."""
    global _pregunta
    with _lock:
        _pregunta = texto
        sys.stdout.write(texto)
        sys.stdout.flush()
    try:
        return input()
    finally:
        with _lock:
            _pregunta = None


class FormatoConsola(logging.Formatter):
    """HH:MM:SS  texto. Los avisos y errores lo indican delante y van en color.

    Un mensaje puede pedir otro color: LOG.info("...", extra={"color": "verde"}).
    """

    def format(self, registro):
        hora = self.formatTime(registro, "%H:%M:%S")
        texto = registro.getMessage()
        if registro.levelno in NOMBRE_NIVEL:
            texto = f"{NOMBRE_NIVEL[registro.levelno]}: {texto}"
        if registro.exc_info:
            texto += "\n" + self.formatException(registro.exc_info)
        color = getattr(registro, "color", None) or COLOR_NIVEL.get(registro.levelno)
        return f"{pintar(hora, 'gris')}  {pintar(texto, color)}"


class ManejadorConsola(logging.Handler):
    def emit(self, registro):
        try:
            escribir(self.format(registro))
        except Exception:  # noqa: BLE001 - un fallo al mostrar no debe parar nada
            self.handleError(registro)


def configurar(nivel=logging.INFO):
    """Llamar una vez al arrancar cada programa, en lugar de logging.basicConfig."""
    global _color
    _color = _activar_color(sys.stdout)
    manejador = ManejadorConsola()
    manejador.setFormatter(FormatoConsola())
    logging.basicConfig(level=nivel, handlers=[manejador])
    # kafka-python escribe varias líneas de ERROR por cada intento fallido. Cada
    # módulo ya avisa con un mensaje propio ("Kafka no disponible..."), así que
    # solo se dejan pasar sus fallos graves.
    logging.getLogger("kafka").setLevel(logging.CRITICAL)

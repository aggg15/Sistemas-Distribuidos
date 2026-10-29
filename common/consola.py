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
_tecleado = ""    # lo que el usuario lleva escrito: se repinta junto a la pregunta
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
            # (salida a un fichero) basta con saltar de línea. Después se vuelve
            # a pintar la pregunta con lo que el usuario llevaba escrito.
            inicio = "\r\x1b[2K" if _color else "\n"
            sys.stdout.write(f"{inicio}{texto}\n{_pregunta}{_tecleado}")
        sys.stdout.flush()


def _teclas_windows():
    import msvcrt
    while True:
        tecla = msvcrt.getwch()
        if tecla in ("\x00", "\xe0"):
            msvcrt.getwch()  # flechas y teclas de función: se ignoran
            continue
        yield tecla


def _teclas_linux():
    import termios
    import tty
    descriptor = sys.stdin.fileno()
    anterior = termios.tcgetattr(descriptor)
    # cbreak: cada tecla llega al momento y sin eco; Ctrl+C sigue funcionando.
    tty.setcbreak(descriptor)
    try:
        while True:
            yield sys.stdin.read(1)
    finally:
        termios.tcsetattr(descriptor, termios.TCSADRAIN, anterior)


def _leer_linea():
    """Lee una línea tecla a tecla guardando lo escrito en _tecleado, para que
    escribir() pueda volver a mostrarlo si llega un mensaje mientras tanto."""
    global _tecleado
    teclas = _teclas_windows() if os.name == "nt" else _teclas_linux()
    try:
        for tecla in teclas:
            if tecla == "\x03":
                raise KeyboardInterrupt
            if tecla in ("\x04", "\x1a", "") and not _tecleado:
                raise EOFError  # Ctrl+D / Ctrl+Z
            with _lock:
                if tecla in ("\r", "\n"):
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    return _tecleado
                if tecla in ("\x08", "\x7f"):  # borrar
                    if _tecleado:
                        _tecleado = _tecleado[:-1]
                        sys.stdout.write("\b \b")
                elif tecla.isprintable():
                    _tecleado += tecla
                    sys.stdout.write(tecla)
                sys.stdout.flush()
        raise EOFError
    finally:
        teclas.close()


def preguntar(texto):
    """input() que los mensajes de otros hilos no tapan ni borran."""
    global _pregunta, _tecleado
    with _lock:
        _pregunta = texto
        _tecleado = ""
        sys.stdout.write(texto)
        sys.stdout.flush()
    try:
        if sys.stdin.isatty() and sys.stdout.isatty():
            return _leer_linea()
        return input()  # sin teclado (pruebas, Docker): lectura normal
    finally:
        with _lock:
            _pregunta = None
            _tecleado = ""


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

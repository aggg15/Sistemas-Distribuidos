"""Los mensajes de otros hilos no deben borrar lo que el usuario está escribiendo."""

import io
import os
import unittest
from unittest.mock import patch

from common import consola


class Terminal(io.StringIO):
    def isatty(self):
        return True


class PruebasPreguntar(unittest.TestCase):
    def preguntar_con(self, teclas):
        salida = Terminal()
        lector = "_teclas_windows" if os.name == "nt" else "_teclas_linux"
        # Generador, como los lectores reales (que se cierran con close()).
        with patch.object(consola, lector, lambda: (t for t in teclas())), \
                patch.object(consola, "_color", True), \
                patch("sys.stdout", salida), patch("sys.stdin", Terminal()):
            respuesta = consola.preguntar("Duración en segundos: ")
        return respuesta, salida.getvalue()

    def test_un_mensaje_a_mitad_no_borra_lo_tecleado(self):
        def teclas():
            yield "2"
            consola.escribir("Regando: 12.0 L/min")  # llega desde otro hilo
            yield "0"
            yield "\r"

        respuesta, salida = self.preguntar_con(teclas)
        self.assertEqual(respuesta, "20")
        # La pregunta se repinta debajo del mensaje con el "2" ya escrito.
        self.assertIn("\r\x1b[2KRegando: 12.0 L/min\nDuración en segundos: 2", salida)

    def test_borrar_y_teclas_especiales(self):
        respuesta, _ = self.preguntar_con(lambda: iter(["3", "0", "\x08", "\t", "5", "\n"]))
        self.assertEqual(respuesta, "35")

    def test_ctrl_c_y_fin_de_entrada(self):
        with self.assertRaises(KeyboardInterrupt):
            self.preguntar_con(lambda: iter(["1", "\x03"]))
        with self.assertRaises(EOFError):
            self.preguntar_con(lambda: iter(["\x1a"]))

    def test_sin_pregunta_activa_escribe_normal(self):
        salida = Terminal()
        with patch("sys.stdout", salida), patch.object(consola, "_color", False):
            consola.escribir("hola")
        self.assertEqual(salida.getvalue(), "hola\n")


if __name__ == "__main__":
    unittest.main()

"""Pruebas del protocolo. Ejecutar desde la raíz: python -m unittest -v"""

import socket
import threading
import time
import unittest

from common.protocolo import (
    ACK, ENQ, EOT, ETX, NACK, STX,
    Canal, ConexionCerrada, ErrorProtocolo, calcular_lrc, empaquetar,
)


class PruebasLRC(unittest.TestCase):
    def test_xor_byte_a_byte(self):
        # 0x41 ^ 0x42 ^ 0x43 = 0x40
        self.assertEqual(calcular_lrc(b"ABC"), 0x40)

    def test_trama_completa(self):
        trama = empaquetar("PING#WS-04")
        self.assertEqual(trama[:1], STX)
        self.assertEqual(trama[-2:-1], ETX)
        self.assertEqual(trama[-1], calcular_lrc(b"PING#WS-04"))

    def test_rechaza_mensajes_vacios_o_enormes(self):
        with self.assertRaises(ValueError):
            empaquetar("")
        with self.assertRaises(ValueError):
            empaquetar("x" * 5000)


class PruebasCanal(unittest.TestCase):
    def setUp(self):
        # Dos sockets ya conectados entre sí: simulan cliente y servidor.
        self.a, self.b = socket.socketpair()
        self.a.settimeout(2)
        self.b.settimeout(2)
        self.cliente = Canal(self.a)
        self.servidor = Canal(self.b)

    def tearDown(self):
        self.a.close()
        self.b.close()

    def en_paralelo(self, funcion):
        errores = []

        def ejecutar():
            try:
                funcion()
            except BaseException as error:
                errores.append(error)

        def comprobar():
            hilo.join(timeout=4)
            self.assertFalse(hilo.is_alive(), "El hilo de prueba quedó bloqueado")
            if errores:
                raise errores[0]

        hilo = threading.Thread(target=ejecutar)
        hilo.start()
        self.addCleanup(comprobar)
        return hilo

    def test_saludo(self):
        hilo = self.en_paralelo(self.servidor.esperar_saludo)
        self.cliente.saludar()
        hilo.join()

    def test_saludo_rechazado(self):
        hilo = self.en_paralelo(lambda: self.servidor.esperar_saludo(aceptar=False))
        with self.assertRaises(ErrorProtocolo):
            self.cliente.saludar()
        hilo.join()

    def test_peticion_y_respuesta_con_acentos(self):
        recibidas = []

        def servidor():
            recibidas.append(self.servidor.responder(lambda texto: "OK#" + texto.split("#")[1]))

        hilo = self.en_paralelo(servidor)
        respuesta = self.cliente.peticion("REGISTRO#WS-07#Jardín Botánico")
        hilo.join()
        self.assertEqual(recibidas, ["REGISTRO#WS-07#Jardín Botánico"])
        self.assertEqual(respuesta, "OK#WS-07")

    def test_varios_ciclos_en_la_misma_conexion(self):
        def servidor():
            for _ in range(3):
                self.servidor.responder(lambda texto: "OK")

        hilo = self.en_paralelo(servidor)
        for _ in range(3):
            self.assertEqual(self.cliente.peticion("PING#WS-04"), "OK")
        hilo.join()

    def test_lrc_incorrecto_provoca_nack_y_reenvio(self):
        recibidas = []
        hilo = self.en_paralelo(lambda: recibidas.append(self.servidor.recibir_trama()))

        # Primera trama corrupta: LRC alterado. El receptor debe responder NACK.
        buena = empaquetar("CHECK")
        self.a.sendall(buena[:-1] + bytes([buena[-1] ^ 0xFF]))
        self.assertEqual(self.a.recv(1), NACK)
        # Reenvío correcto: ahora ACK.
        self.a.sendall(buena)
        self.assertEqual(self.a.recv(1), ACK)
        hilo.join()
        self.assertEqual(recibidas, ["CHECK"])

    def test_emisor_desiste_tras_tres_nack(self):
        def receptor_que_siempre_rechaza():
            for _ in range(3):
                self.b.recv(1024)
                self.b.sendall(NACK)

        hilo = self.en_paralelo(receptor_que_siempre_rechaza)
        with self.assertRaises(ErrorProtocolo):
            self.cliente.enviar_trama("CHECK")
        hilo.join()

    def test_eot_es_cierre_ordenado(self):
        self.cliente.cerrar()
        with self.assertRaises(ConexionCerrada):
            self.servidor.recibir_trama()

    def test_cierre_sin_eot_es_un_fallo(self):
        self.a.close()
        with self.assertRaises(ConnectionError):
            self.servidor.recibir_trama()

    def test_basura_en_lugar_de_stx(self):
        self.a.sendall(b"hola")
        with self.assertRaises(ErrorProtocolo):
            self.servidor.recibir_trama()

    def test_saludo_sin_enq(self):
        self.a.sendall(STX)
        with self.assertRaises(ErrorProtocolo):
            self.servidor.esperar_saludo()

    def test_texto_invalido_recibe_nack_antes_de_ser_confirmado(self):
        # El checksum puede ser correcto aunque el contenido no sea UTF-8.
        errores = []

        def recibir():
            try:
                self.servidor.recibir_trama()
            except ErrorProtocolo as error:
                errores.append(error)

        hilo = self.en_paralelo(recibir)
        for datos in (b"\xff", b"", b"A\x02B"):
            self.a.sendall(STX + datos + ETX + bytes([calcular_lrc(datos)]))
            self.assertEqual(self.a.recv(1), NACK)
        hilo.join(timeout=1)
        self.assertFalse(hilo.is_alive())
        self.assertEqual(len(errores), 1)

    def test_lrc_puede_ser_un_byte_de_control(self):
        # XOR de A y B es ETX: no debe confundirse el checksum con un delimitador.
        recibidas = []
        hilo = self.en_paralelo(lambda: recibidas.append(self.servidor.recibir_trama()))
        self.cliente.enviar_trama("AB")
        hilo.join(timeout=1)
        self.assertEqual(recibidas, ["AB"])

    def test_plazo_total_incluso_si_llegan_bytes_poco_a_poco(self):
        self.servidor = Canal(self.b, timeout=0.12)
        terminar = threading.Event()

        def emisor_lento():
            self.a.sendall(STX)
            while not terminar.wait(0.02):
                self.a.sendall(b"x")

        hilo = self.en_paralelo(emisor_lento)
        inicio = time.monotonic()
        try:
            with self.assertRaises(TimeoutError):
                self.servidor.recibir_trama()
            self.assertLess(time.monotonic() - inicio, 0.8)
        finally:
            terminar.set()
            hilo.join(timeout=1)

    def test_timeout_esperando_ack(self):
        self.cliente = Canal(self.a, timeout=0.05)
        with self.assertRaises(TimeoutError):
            self.cliente.enviar_trama("PING#WS-04")

    def test_timeout_esperando_lrc(self):
        self.servidor = Canal(self.b, timeout=0.05)
        self.a.sendall(STX + b"CHECK" + ETX)
        with self.assertRaises(TimeoutError):
            self.servidor.recibir_trama()


if __name__ == "__main__":
    unittest.main()

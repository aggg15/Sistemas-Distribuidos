"""Integración de los programas reales sobre sockets conectados, sin broker."""

from concurrent.futures import Future
from contextlib import nullcontext
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from WM_Central import CentralTCP
from WM_WS_M import RegistroRechazado, ejecutar, mantener_sesion
from common.protocolo import ACK, NACK, STX, Canal, ConexionCerrada, empaquetar


class PruebasCentral(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.central = CentralTCP(timeout=0.5, ruta_bd=f"{temporal.name}/wm.db")

    def conectar(self, saludo=True):
        cliente, servidor = socket.socketpair()
        cliente.settimeout(1)
        resultado = Future()

        def atender():
            try:
                self.central.atender(servidor, "prueba")
                resultado.set_result(None)
            except BaseException as error:
                resultado.set_exception(error)

        hilo = threading.Thread(target=atender)
        hilo.start()

        def limpiar():
            cliente.close()
            resultado.result(timeout=2)
            hilo.join(timeout=1)
            self.assertFalse(hilo.is_alive())

        self.addCleanup(limpiar)
        canal = Canal(cliente, timeout=1)
        if saludo:
            canal.saludar()
        return cliente, canal, resultado

    def test_registro_y_varios_latidos_en_la_misma_conexion(self):
        _, canal, resultado = self.conectar()
        self.assertEqual(canal.peticion("REGISTRO#WS-07#Jardín Botánico"), "OK#WS-07")
        for _ in range(3):
            self.assertEqual(canal.peticion("PING#WS-07"), "OK")
        self.assertTrue(self.central.registro.estado()["WS-07"]["conectada"])
        canal.cerrar()
        resultado.result(timeout=1)
        self.assertFalse(self.central.registro.estado()["WS-07"]["conectada"])

    def test_registro_invalido_es_rechazado_y_cierra(self):
        for peticion in ("PING#WS-01", "REGISTRO##Parque", "REGISTRO#WS-01# ",
                         "REGISTRO#WS-01#Parque#otro", "REGISTRO#WS-01#Parque\n"):
            with self.subTest(peticion=peticion):
                _, canal, resultado = self.conectar()
                self.assertTrue(canal.peticion(peticion).startswith("KO#"))
                with self.assertRaises(ConexionCerrada):
                    canal.recibir_trama()
                resultado.result(timeout=1)
        self.assertEqual(self.central.registro.estado(), {})

    def test_id_duplicado_no_desconecta_al_monitor_original(self):
        _, original, _ = self.conectar()
        self.assertEqual(original.peticion("REGISTRO#WS-01#Parque"), "OK#WS-01")
        _, duplicado, resultado = self.conectar()
        self.assertTrue(duplicado.peticion("REGISTRO#WS-01#Otro").startswith("KO#"))
        resultado.result(timeout=1)
        self.assertEqual(original.peticion("PING#WS-01"), "OK")
        self.assertEqual(self.central.registro.estado()["WS-01"]["ubicacion"], "Parque")

    def test_reconexion_tras_cierre_abrupto(self):
        socket_cliente, canal, resultado = self.conectar()
        canal.peticion("REGISTRO#WS-01#Parque")
        socket_cliente.close()
        resultado.result(timeout=1)
        _, nuevo, _ = self.conectar()
        self.assertEqual(nuevo.peticion("REGISTRO#WS-01#Jardín"), "OK#WS-01")

    def test_averia_resuelta_y_rechazo_de_id_ajeno(self):
        _, canal, _ = self.conectar()
        canal.peticion("REGISTRO#WS-01#Parque")
        self.assertTrue(canal.peticion("AVERIA#WS-99").startswith("KO#"))
        self.assertFalse(self.central.registro.estado()["WS-01"]["averia"])
        self.assertEqual(canal.peticion("AVERIA#WS-01"), "OK")
        self.assertTrue(self.central.registro.estado()["WS-01"]["averia"])
        self.assertEqual(canal.peticion("RESUELTA#WS-01"), "OK")
        self.assertFalse(self.central.registro.estado()["WS-01"]["averia"])
        self.assertTrue(canal.peticion("OTRA#WS-01").startswith("KO#"))
        self.assertEqual(canal.peticion("PING#WS-01"), "OK")

    def test_trama_corrupta_se_reenvia_sin_perder_la_sesion(self):
        conexion, canal, _ = self.conectar()
        trama = empaquetar("REGISTRO#WS-01#Parque")
        conexion.sendall(trama[:-1] + bytes([trama[-1] ^ 255]))
        self.assertEqual(conexion.recv(1), NACK)
        self.assertEqual(canal.peticion("REGISTRO#WS-01#Parque"), "OK#WS-01")

    def test_fragmentacion_en_la_central_real(self):
        conexion, canal, _ = self.conectar()
        trama = empaquetar("REGISTRO#WS-01#Jardín")
        for byte in trama:
            conexion.sendall(bytes([byte]))
        self.assertEqual(conexion.recv(1), ACK)
        self.assertEqual(canal.recibir_trama(), "OK#WS-01")

    def test_conexion_incompleta_no_bloquea_otras(self):
        self.central.timeout = 0.15
        incompleto, _, resultado = self.conectar()
        incompleto.sendall(STX + b"REG")
        _, valido, _ = self.conectar()
        self.assertEqual(valido.peticion("REGISTRO#WS-02#Parque"), "OK#WS-02")
        resultado.result(timeout=1)
        self.assertNotIn("WS-01", self.central.registro.estado())

    def test_timeout_de_monitor_registrado_libera_su_id(self):
        self.central.timeout = 0.1
        _, canal, resultado = self.conectar()
        canal.peticion("REGISTRO#WS-01#Parque")
        resultado.result(timeout=1)
        self.assertFalse(self.central.registro.estado()["WS-01"]["conectada"])

    def test_timeout_de_saludo(self):
        self.central.timeout = 0.05
        _, _, resultado = self.conectar(saludo=False)
        resultado.result(timeout=1)


class PruebasMonitor(unittest.TestCase):
    def setUp(self):
        self.parada = threading.Event()
        self.cliente, self.servidor = socket.socketpair()
        self.addCleanup(self.cliente.close)
        self.addCleanup(self.servidor.close)

    def central_simulada(self, rechazar=False):
        resultado = Future()

        def ejecutar_servidor():
            try:
                canal = Canal(self.servidor, timeout=1)
                canal.esperar_saludo()
                recibidas = []

                def procesar(texto):
                    recibidas.append(texto)
                    return "OK#WS-01" if texto.startswith("REGISTRO#") else "OK"

                if rechazar:
                    canal.responder(lambda _: "KO#ID duplicado")
                else:
                    for _ in range(3):
                        canal.responder(procesar)
                    self.parada.set()
                with self.assertRaises(ConexionCerrada):
                    canal.recibir_trama()
                resultado.set_result(recibidas)
            except BaseException as error:
                resultado.set_exception(error)

        hilo = threading.Thread(target=ejecutar_servidor)
        hilo.start()

        def limpiar():
            self.cliente.close()
            hilo.join(timeout=2)
            self.assertFalse(hilo.is_alive())

        self.addCleanup(limpiar)
        return resultado

    def test_monitor_registra_envia_latidos_y_cierra_con_eot(self):
        resultado = self.central_simulada()
        mantener_sesion(self.cliente, "WS-01", "Parque", self.parada,
                        intervalo=0.01, timeout=1)
        self.assertEqual(resultado.result(timeout=1),
                         ["REGISTRO#WS-01#Parque", "PING#WS-01", "PING#WS-01"])

    def test_monitor_reintenta_una_conexion_fallida(self):
        resultado = self.central_simulada()
        with patch("WM_WS_M.socket.create_connection",
                   side_effect=[ConnectionRefusedError("Central apagada"), self.cliente]) as conectar:
            codigo = ejecutar("central", 9999, "WS-01", "Parque", self.parada,
                              intervalo=0.01, timeout=1, reintento=0.01)
        self.assertEqual(codigo, 0)
        self.assertEqual(conectar.call_count, 2)
        self.assertEqual(len(resultado.result(timeout=1)), 3)

    def test_rechazo_de_registro_da_codigo_uno_sin_reintentar(self):
        resultado = self.central_simulada(rechazar=True)
        with patch("WM_WS_M.socket.create_connection", return_value=self.cliente) as conectar:
            codigo = ejecutar("central", 9999, "WS-01", "Parque", self.parada, timeout=1)
        self.assertEqual(codigo, 1)
        self.assertEqual(conectar.call_count, 1)
        resultado.result(timeout=1)

    def test_reconexion_espera_a_que_caduque_la_sesion_anterior(self):
        intentos = []

        def sesion(conexion, ws_id, ubicacion, parada, intervalo, timeout, confirmado,
                   salud=None, al_registrar=None):
            intentos.append(None)
            if len(intentos) == 1:
                confirmado.set()
                raise ConnectionResetError("Corte tras el registro")
            if len(intentos) == 2:
                raise RegistroRechazado("ID_EN_USO")
            parada.set()

        with patch("WM_WS_M.socket.create_connection", side_effect=lambda *a, **k: nullcontext()), \
                patch("WM_WS_M.mantener_sesion", side_effect=sesion):
            codigo = ejecutar("central", 9999, "WS-01", "Parque", self.parada,
                              timeout=1, reintento=0.01)
        self.assertEqual(codigo, 0)
        self.assertEqual(len(intentos), 3)


if __name__ == "__main__":
    unittest.main()

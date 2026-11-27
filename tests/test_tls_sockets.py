"""Canal TLS entre CENTRAL y los Monitores (Issue #6, ARCHITECTURE.md D2).

Sigue el ejemplo de sockets seguros de la asignatura (SSLContext +
wrap_socket en servidor y cliente) con una diferencia: el Monitor verifica el
certificado de CENTRAL con la CA del proyecto en lugar de usar
ssl._create_unverified_context(), que dejaría pasar a un MITM.
"""

from argparse import Namespace
from concurrent.futures import Future
from contextlib import closing
from unittest.mock import patch
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import ssl
import tempfile
import threading
import unittest
import warnings

from WM_Central import CentralTCP, cargar_tls
from WM_WS_M import cargar_tls as cargar_tls_monitor, conectar, ejecutar, mantener_sesion
from common.protocolo import Canal, ConexionCerrada, ErrorProtocolo

try:
    from common.certificados import generar_en
    from common.seguridad import contexto_tls_cliente, contexto_tls_servidor
    HAY_CRYPTOGRAPHY = True
except ImportError:
    HAY_CRYPTOGRAPHY = False


class Espia:
    """Reenvía bytes entre el Monitor y CENTRAL y guarda lo que ve: como un MITM pasivo."""

    def __init__(self, puerto_destino):
        self.visto = bytearray()
        self._servidor = socket.create_server(("127.0.0.1", 0))
        self.puerto = self._servidor.getsockname()[1]
        self._destino = puerto_destino
        self._sockets, self._hilos = [], []
        self._aceptador = threading.Thread(target=self._aceptar, daemon=True)
        self._aceptador.start()

    def _aceptar(self):
        cliente, _ = self._servidor.accept()
        servidor = socket.create_connection(("127.0.0.1", self._destino))
        self._sockets += [cliente, servidor]
        for origen, destino in ((cliente, servidor), (servidor, cliente)):
            hilo = threading.Thread(target=self._bombear, args=(origen, destino), daemon=True)
            self._hilos.append(hilo)
            hilo.start()

    def _bombear(self, origen, destino):
        try:
            while True:
                datos = origen.recv(4096)
                if not datos:
                    break
                self.visto.extend(datos)
                destino.sendall(datos)
        except OSError:
            pass
        finally:
            for extremo in (origen, destino):
                try:
                    extremo.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

    def cerrar(self):
        self._aceptador.join(2)
        for hilo in self._hilos:
            hilo.join(2)
        for extremo in [self._servidor, *self._sockets]:
            extremo.close()


@unittest.skipUnless(HAY_CRYPTOGRAPHY, "Hace falta cryptography (requirements.txt)")
class PruebasTlsSockets(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.carpeta = Path(tempfile.mkdtemp())
        cls.certs = generar_en(cls.carpeta / "certs", ["127.0.0.1", "localhost"])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.carpeta, ignore_errors=True)

    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.ruta_bd = f"{temporal.name}/wm.db"
        self.central = CentralTCP(host="127.0.0.1", puerto_escucha=0, timeout=1.0,
                                  ruta_bd=self.ruta_bd,
                                  contexto_tls=contexto_tls_servidor(self.certs["cert"],
                                                                     self.certs["key"]))
        self.central.bd.registrar_estacion("WS-01", "Parque")
        hilo = threading.Thread(target=self.central.servir, daemon=True)
        hilo.start()
        self.assertTrue(self.central.lista.wait(3))
        self.addCleanup(hilo.join, 5)
        self.addCleanup(self.central.detener)
        self.parada = threading.Event()
        self.addCleanup(self.parada.set)

    def conectada(self):
        return self.central.registro.estado().get("WS-01", {}).get("conectada", False)

    def sesion_en_hilo(self, conexion):
        """Monitor real (mantener_sesion) en un hilo; devuelve (registrado, resultado)."""
        registrado, resultado = threading.Event(), Future()

        def correr():
            try:
                with conexion:
                    mantener_sesion(conexion, "WS-01", "Parque", self.parada, intervalo=0.05,
                                    timeout=1.0, registro_confirmado=registrado)
                resultado.set_result(None)
            except BaseException as error:
                resultado.set_exception(error)

        threading.Thread(target=correr, daemon=True).start()
        return registrado, resultado

    def test_monitor_con_la_ca_del_proyecto_se_registra_por_tls(self):
        conexion = conectar("127.0.0.1", self.central.puerto, 2,
                            contexto_tls_cliente(self.certs["ca"]))
        self.assertIn(conexion.version(), ("TLSv1.2", "TLSv1.3"))
        registrado, resultado = self.sesion_en_hilo(conexion)
        self.assertTrue(registrado.wait(3))
        self.assertTrue(self.conectada())
        self.parada.set()
        resultado.result(timeout=3)

    def test_quien_escucha_la_red_no_ve_ids_ni_mensajes(self):
        espia = Espia(self.central.puerto)
        self.addCleanup(espia.cerrar)
        conexion = conectar("127.0.0.1", espia.puerto, 2, contexto_tls_cliente(self.certs["ca"]))
        registrado, resultado = self.sesion_en_hilo(conexion)
        self.assertTrue(registrado.wait(3))
        self.parada.set()
        resultado.result(timeout=3)
        self.assertGreater(len(espia.visto), 0)
        for en_claro in (b"REGISTRO", b"WS-01", b"Parque", b"PING"):
            self.assertNotIn(en_claro, bytes(espia.visto))

    def test_parar_central_cierra_la_sesion_con_eot_cifrado(self):
        conexion = conectar("127.0.0.1", self.central.puerto, 2,
                            contexto_tls_cliente(self.certs["ca"]))
        registrado, resultado = self.sesion_en_hilo(conexion)
        self.assertTrue(registrado.wait(3))
        self.central.detener()
        # El Monitor recibe el EOT dentro de TLS: cierre ordenado, no un error de TLS.
        with self.assertRaises(ConexionCerrada):
            resultado.result(timeout=5)

    def test_monitor_con_otra_ca_no_se_conecta(self):
        otra = generar_en(self.carpeta / "otra", ["127.0.0.1"])
        codigo = ejecutar("127.0.0.1", self.central.puerto, "WS-01", "Parque", self.parada,
                          timeout=1, reintento=0.1, contexto_tls=contexto_tls_cliente(otra["ca"]))
        self.assertEqual(codigo, 1, "un certificado no fiable no se reintenta")
        self.assertFalse(self.conectada())

    def test_certificado_para_otra_ip_es_rechazado(self):
        # El Monitor cree hablar con 192.0.2.1 pero llega a esta CENTRAL, cuyo
        # certificado es para 127.0.0.1: la IP no coincide y se corta.
        original = socket.create_connection
        puerto = self.central.puerto
        with patch("WM_WS_M.socket.create_connection",
                   lambda _, timeout: original(("127.0.0.1", puerto), timeout=timeout)):
            with self.assertRaises(ssl.SSLCertVerificationError):
                conectar("192.0.2.1", puerto, 2, contexto_tls_cliente(self.certs["ca"]))

    def test_tls_anterior_a_1_2_es_rechazado(self):
        contexto = contexto_tls_cliente(self.certs["ca"])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # TLS 1.1 está obsoleto
            contexto.minimum_version = ssl.TLSVersion.MINIMUM_SUPPORTED
            contexto.maximum_version = ssl.TLSVersion.TLSv1_1
        with self.assertRaises(ssl.SSLError):
            conectar("127.0.0.1", self.central.puerto, 2, contexto)

    def test_monitor_sin_tls_es_rechazado_y_auditado(self):
        with socket.create_connection(("127.0.0.1", self.central.puerto), timeout=2) as crudo:
            canal = Canal(crudo, timeout=2)
            with self.assertRaises((ConexionCerrada, ErrorProtocolo, OSError)):
                canal.saludar()
                canal.peticion("REGISTRO#WS-01#Parque")
        self.assertFalse(self.conectada())
        # El hilo de la conexión audita el fallo en cuanto vence el handshake.
        for _ in range(40):
            with closing(sqlite3.connect(self.ruta_bd)) as bd:
                filas = bd.execute("SELECT accion, resultado, ip, detalle FROM auditoria").fetchall()
            if filas:
                break
            threading.Event().wait(0.05)
        self.assertEqual(len(filas), 1)
        accion, resultado, ip, detalle = filas[0]
        self.assertEqual((accion, resultado, ip), ("AUTH_KO", "KO", "127.0.0.1"))
        self.assertEqual(json.loads(detalle)["motivo"], "HANDSHAKE_TLS")


@unittest.skipUnless(HAY_CRYPTOGRAPHY, "Hace falta cryptography (requirements.txt)")
class PruebasConfiguracionTls(unittest.TestCase):
    def setUp(self):
        self.carpeta = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.carpeta, True)
        self.certs = generar_en(self.carpeta, ["127.0.0.1"])

    class Parser:
        def exit(self, codigo, mensaje):
            raise SystemExit(mensaje)

    def args(self, **valores):
        return Namespace(**{"cert": None, "key": None, "sin_tls": False, **valores})

    def test_certificado_y_clave_en_un_solo_fichero_como_el_ejemplo(self):
        # El ejemplo de la asignatura usa load_cert_chain(cert, cert): un único PEM.
        combinado = self.carpeta / "certServ.pem"
        combinado.write_bytes(self.certs["cert"].read_bytes() + self.certs["key"].read_bytes())
        contexto = cargar_tls(self.Parser(), self.args(cert=str(combinado)))
        self.assertIsInstance(contexto, ssl.SSLContext)

    def test_sin_tls_y_ficheros_que_no_existen(self):
        self.assertIsNone(cargar_tls(self.Parser(), self.args(sin_tls=True)))
        with self.assertRaises(SystemExit):
            cargar_tls(self.Parser(), self.args(cert=str(self.carpeta / "no-existe.pem")))

    def test_sin_certificados_central_no_arranca_en_claro(self):
        with patch("WM_Central.CERT_TLS", str(self.carpeta / "no.pem")), \
                patch("WM_Central.CLAVE_TLS", str(self.carpeta / "no.key")):
            with self.assertRaises(SystemExit):
                cargar_tls(self.Parser(), self.args())

    def test_monitor_sin_ca_no_arranca_en_claro(self):
        args = Namespace(sin_tls=False, ca=None, sin_comprobar_nombre=False)
        with patch("WM_WS_M.CA_TLS", str(self.carpeta / "no-ca.pem")):
            with self.assertRaises(SystemExit):
                cargar_tls_monitor(self.Parser(), args)
        args.ca = str(self.certs["ca"])
        self.assertEqual(cargar_tls_monitor(self.Parser(), args).verify_mode, ssl.CERT_REQUIRED)


if __name__ == "__main__":
    unittest.main()

"""Credenciales, cifrado Fernet, certificados y TLS de Release 2 (#2)."""

from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from common import seguridad
from common.seguridad import (MensajeNoValido, cifrar, contexto_tls_cliente,
                              contexto_tls_servidor, descifrar, generar_clave,
                              generar_credencial, hash_credencial, verificar_credencial)

try:
    from common.certificados import generar_en
except ImportError:  # sin cryptography instalada
    generar_en = None

RAIZ = Path(__file__).resolve().parent.parent


class PruebasCredenciales(unittest.TestCase):
    def test_credencial_aleatoria_y_apta_para_el_protocolo(self):
        credenciales = {generar_credencial() for _ in range(50)}
        self.assertEqual(len(credenciales), 50)
        for credencial in credenciales:
            self.assertGreaterEqual(len(credencial), 43)
            self.assertNotIn("#", credencial)  # separador del protocolo de sockets
            self.assertTrue(credencial.isprintable())

    def test_hash_y_verificacion(self):
        credencial = generar_credencial()
        almacenado = hash_credencial(credencial)
        self.assertTrue(almacenado.startswith("pbkdf2_sha256$200000$"))
        self.assertNotIn(credencial, almacenado)
        self.assertTrue(verificar_credencial(credencial, almacenado))
        self.assertFalse(verificar_credencial(credencial + "x", almacenado))
        self.assertFalse(verificar_credencial(generar_credencial(), almacenado))

    def test_misma_credencial_distinta_sal(self):
        self.assertNotEqual(hash_credencial("secreto"), hash_credencial("secreto"))

    def test_datos_mal_formados_no_validan_ni_fallan(self):
        for almacenado in (None, "", "basura", "md5$1$aa$bb", "pbkdf2_sha256$x$aa$bb",
                           "pbkdf2_sha256$1000$%%%$bb"):
            with self.subTest(almacenado=almacenado):
                self.assertFalse(verificar_credencial("secreto", almacenado))
        self.assertFalse(verificar_credencial("", hash_credencial("secreto", iteraciones=1000)))
        self.assertFalse(verificar_credencial(None, hash_credencial("secreto", iteraciones=1000)))
        with self.assertRaises(ValueError):
            hash_credencial("")

    def test_comparacion_en_tiempo_constante(self):
        almacenado = hash_credencial("secreto", iteraciones=1000)
        with patch.object(seguridad.hmac, "compare_digest", wraps=seguridad.hmac.compare_digest) as cmp:
            verificar_credencial("secreto", almacenado)
        cmp.assert_called_once()


class PruebasCifrado(unittest.TestCase):
    MENSAJE = {"tipo": "INICIAR_RIEGO", "ws_id": "WS-04", "riego_id": 7,
               "duracion_s": 20, "texto": "Válvula ñ"}

    def setUp(self):
        self.clave = generar_clave()

    def test_ida_y_vuelta(self):
        token = cifrar(self.clave, self.MENSAJE)
        self.assertIsInstance(token, str)
        self.assertNotIn("INICIAR_RIEGO", token)
        self.assertNotIn("WS-04", token)
        self.assertEqual(descifrar(self.clave, token), self.MENSAJE)

    def test_claves_distintas_por_estacion(self):
        self.assertNotEqual(generar_clave(), generar_clave())

    def test_mismo_mensaje_cifrado_distinto_cada_vez(self):
        self.assertNotEqual(cifrar(self.clave, self.MENSAJE), cifrar(self.clave, self.MENSAJE))

    def test_otra_clave_no_descifra(self):
        token = cifrar(self.clave, self.MENSAJE)
        with self.assertRaises(MensajeNoValido):
            descifrar(generar_clave(), token)

    def test_cualquier_byte_manipulado_se_detecta(self):
        token = cifrar(self.clave, self.MENSAJE)
        for posicion in (10, len(token) // 2, len(token) - 5):
            alterado = token[:posicion] + ("A" if token[posicion] != "A" else "B") + token[posicion + 1:]
            with self.subTest(posicion=posicion), self.assertRaises(MensajeNoValido):
                descifrar(self.clave, alterado)

    def test_caducidad(self):
        token = cifrar(self.clave, self.MENSAJE)
        self.assertEqual(descifrar(self.clave, token, ttl=60), self.MENSAJE)
        with patch("time.time", return_value=time.time() + 120), self.assertRaises(MensajeNoValido):
            descifrar(self.clave, token, ttl=60)

    def test_entradas_no_validas(self):
        for token in ("", None, 123, "no es un token", "ñ"):
            with self.subTest(token=token), self.assertRaises(MensajeNoValido):
                descifrar(self.clave, token)
        for clave in ("corta", "", None):
            with self.subTest(clave=clave), self.assertRaises(MensajeNoValido):
                cifrar(clave, self.MENSAJE)
        with self.assertRaises(TypeError):
            cifrar(self.clave, ["no", "es", "dict"])

    def test_contenido_que_no_es_objeto_json(self):
        from cryptography.fernet import Fernet
        fernet = Fernet(self.clave)
        for contenido in (b"[1, 2]", b"no json", b"\xff\xfe"):
            token = fernet.encrypt(contenido).decode()
            with self.subTest(contenido=contenido), self.assertRaises(MensajeNoValido):
                descifrar(self.clave, token)


@unittest.skipIf(generar_en is None, "Instala requirements.txt (cryptography)")
class PruebasTLS(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.carpeta = Path(temporal.name)
        self.rutas = generar_en(self.carpeta / "buena", ["127.0.0.1", "localhost"])

    def conectar(self, contexto_cliente, nombre="127.0.0.1"):
        """Servidor TLS de un solo uso que contesta 'hola'. Devuelve lo recibido."""
        servidor_ctx = contexto_tls_servidor(self.rutas["cert"], self.rutas["key"])
        escucha = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(escucha.close)
        escucha.settimeout(3)

        def atender():
            try:
                conexion, _ = escucha.accept()
                with servidor_ctx.wrap_socket(conexion, server_side=True) as tls:
                    tls.sendall(b"hola")
            except (OSError, ssl.SSLError):
                pass  # el cliente rechazó el certificado
        hilo = threading.Thread(target=atender, daemon=True)
        hilo.start()
        try:
            with socket.create_connection(escucha.getsockname(), timeout=3) as crudo:
                with contexto_cliente.wrap_socket(crudo, server_hostname=nombre) as tls:
                    return tls.recv(4), tls.version()
        finally:
            hilo.join(3)

    def test_cliente_con_la_ca_del_proyecto(self):
        datos, version = self.conectar(contexto_tls_cliente(self.rutas["ca"]))
        self.assertEqual(datos, b"hola")
        self.assertIn(version, ("TLSv1.2", "TLSv1.3"))

    def test_cliente_con_otra_ca_rechaza_el_servidor(self):
        otra = generar_en(self.carpeta / "otra", ["127.0.0.1"])
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.conectar(contexto_tls_cliente(otra["ca"]))

    def test_nombre_que_no_esta_en_el_certificado(self):
        with self.assertRaises(ssl.SSLCertVerificationError):
            self.conectar(contexto_tls_cliente(self.rutas["ca"]), nombre="10.9.9.9")
        datos, _ = self.conectar(contexto_tls_cliente(self.rutas["ca"], comprobar_nombre=False),
                                 nombre="10.9.9.9")
        self.assertEqual(datos, b"hola")

    def test_version_minima_tls12(self):
        for contexto in (contexto_tls_cliente(self.rutas["ca"]),
                         contexto_tls_servidor(self.rutas["cert"], self.rutas["key"])):
            self.assertEqual(contexto.minimum_version, ssl.TLSVersion.TLSv1_2)
        self.assertEqual(contexto_tls_cliente(self.rutas["ca"]).verify_mode, ssl.CERT_REQUIRED)

    def test_ca_se_reutiliza_y_el_servidor_se_renueva(self):
        ca_antes = self.rutas["ca"].read_bytes()
        cert_antes = self.rutas["cert"].read_bytes()
        generar_en(self.carpeta / "buena", ["127.0.0.1", "3.80.1.2"])
        self.assertEqual(self.rutas["ca"].read_bytes(), ca_antes)
        self.assertNotEqual(self.rutas["cert"].read_bytes(), cert_antes)
        # El certificado nuevo sigue validando con la misma CA: no hay que redistribuirla.
        self.assertEqual(self.conectar(contexto_tls_cliente(self.rutas["ca"]))[0], b"hola")
        generar_en(self.carpeta / "buena", ["127.0.0.1"], nueva_ca=True)
        self.assertNotEqual(self.rutas["ca"].read_bytes(), ca_antes)

    @unittest.skipIf(sys.platform == "win32", "Windows no usa permisos POSIX")
    def test_claves_privadas_solo_legibles_por_el_propietario(self):
        for clave in (self.rutas["key"], self.rutas["ca_key"]):
            self.assertEqual(clave.stat().st_mode & 0o777, 0o600)

    @unittest.skipIf(sys.platform == "win32", "Windows no usa permisos POSIX")
    def test_regenerar_corrige_permisos_de_una_clave_existente(self):
        carpeta = self.carpeta / "permisos"
        carpeta.mkdir()
        clave = carpeta / "servidor.key"
        clave.write_text("antigua")
        clave.chmod(0o644)
        generar_en(carpeta, ["127.0.0.1"])
        self.assertEqual(clave.stat().st_mode & 0o777, 0o600)

    def test_script_de_generacion(self):
        carpeta = self.carpeta / "script"
        salida = subprocess.run([sys.executable, str(RAIZ / "scripts" / "generar_certificados.py"),
                                 "3.80.1.2", "--carpeta", str(carpeta)],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(salida.returncode, 0, salida.stderr)
        self.assertIn("3.80.1.2, localhost, 127.0.0.1", salida.stdout)
        for nombre in ("ca.pem", "ca.key", "servidor.pem", "servidor.key"):
            self.assertTrue((carpeta / nombre).exists())

    def test_git_ignora_certificados_y_credenciales(self):
        rutas = ["certs/servidor.key", "certs/ca.pem", "data/ws_WS-04.cred", "otra/clave.key"]
        salida = subprocess.run(["git", "check-ignore", *rutas], cwd=RAIZ,
                                capture_output=True, text=True)
        if salida.returncode == 128:
            self.skipTest("No es un repositorio Git")
        self.assertEqual(salida.stdout.split(), rutas)


if __name__ == "__main__":
    unittest.main()

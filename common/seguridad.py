"""Primitivas de seguridad de Release 2 (docs/release2/ARCHITECTURE.md §4, D1–D3).

- Credenciales de WM_Registry: aleatorias, guardadas solo como hash PBKDF2.
- Cifrado simétrico de mensajes: Fernet (AES-128-CBC + HMAC-SHA256), una clave por WS.
- Contextos TLS para HTTPS del Registry y los sockets CENTRAL ↔ Monitor.

Las credenciales y el hash solo usan la biblioteca estándar. El cifrado necesita
`cryptography` (requirements.txt): se importa al usarlo para que el resto de
módulos funcione aunque no esté instalado.
"""

import base64
import hashlib
import hmac
import json
import secrets
import ssl

ITERACIONES_PBKDF2 = 200_000
BYTES_SAL = 16
ALGORITMO_HASH = "pbkdf2_sha256"


class MensajeNoValido(Exception):
    """Token manipulado, cifrado con otra clave, caducado o con contenido no válido."""


# --- Credenciales -----------------------------------------------------------

def generar_credencial():
    """Credencial de 256 bits en base64 URL (43 caracteres, sin '#')."""
    return secrets.token_urlsafe(32)


def _b64(datos):
    return base64.urlsafe_b64encode(datos).decode("ascii").rstrip("=")


def _desde_b64(texto):
    return base64.urlsafe_b64decode(texto + "=" * (-len(texto) % 4))


def hash_credencial(credencial, iteraciones=ITERACIONES_PBKDF2):
    """Devuelve 'pbkdf2_sha256$iteraciones$sal$hash'. Cada llamada usa una sal nueva."""
    if not credencial:
        raise ValueError("Credencial vacía")
    sal = secrets.token_bytes(BYTES_SAL)
    resumen = hashlib.pbkdf2_hmac("sha256", credencial.encode("utf-8"), sal, iteraciones)
    return f"{ALGORITMO_HASH}${iteraciones}${_b64(sal)}${_b64(resumen)}"


def verificar_credencial(credencial, almacenado):
    """True si la credencial corresponde al hash. False ante cualquier dato mal formado."""
    if not credencial or not almacenado:
        return False
    try:
        algoritmo, iteraciones, sal, esperado = almacenado.split("$")
        if algoritmo != ALGORITMO_HASH:
            return False
        resumen = hashlib.pbkdf2_hmac("sha256", credencial.encode("utf-8"),
                                      _desde_b64(sal), int(iteraciones))
        # Comparación en tiempo constante: no revela cuántos bytes coinciden.
        return hmac.compare_digest(resumen, _desde_b64(esperado))
    except (ValueError, TypeError):
        return False


# --- Cifrado simétrico de mensajes (Fernet) ---------------------------------

def _fernet(clave):
    from cryptography.fernet import Fernet  # dependencia de requirements.txt
    try:
        return Fernet(clave)
    except (ValueError, TypeError) as error:
        raise MensajeNoValido("Clave de cifrado no válida") from error


def generar_clave():
    """Clave Fernet nueva (32 bytes en base64 URL, como texto)."""
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode("ascii")


def cifrar(clave, mensaje):
    """Cifra un diccionario JSON. Devuelve el token como texto."""
    if not isinstance(mensaje, dict):
        raise TypeError("Solo se cifran mensajes JSON (diccionarios)")
    datos = json.dumps(mensaje, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return _fernet(clave).encrypt(datos).decode("ascii")


def descifrar(clave, token, ttl=None):
    """Descifra y valida un token. ttl: segundos máximos de antigüedad (None = sin límite).

    Cualquier fallo (clave errónea, manipulación, caducidad, JSON inválido o
    que no sea un objeto) lanza MensajeNoValido y nunca devuelve datos parciales.
    """
    from cryptography.fernet import InvalidToken
    if not isinstance(token, str) or not token:
        raise MensajeNoValido("Token vacío o no textual")
    try:
        datos = _fernet(clave).decrypt(token.encode("ascii"), ttl=ttl)
    except (InvalidToken, UnicodeEncodeError) as error:
        raise MensajeNoValido("Token no válido, manipulado, caducado o de otra clave") from error
    try:
        mensaje = json.loads(datos.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as error:
        raise MensajeNoValido("El contenido descifrado no es JSON") from error
    if not isinstance(mensaje, dict):
        raise MensajeNoValido("El contenido descifrado no es un objeto JSON")
    return mensaje


# --- TLS --------------------------------------------------------------------

def contexto_tls_servidor(certificado, clave_privada):
    """Contexto para Registry (HTTPS) y CENTRAL (sockets). TLS 1.2 como mínimo."""
    contexto = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    contexto.minimum_version = ssl.TLSVersion.TLSv1_2
    contexto.load_cert_chain(certificado, clave_privada)
    return contexto


def contexto_tls_cliente(ca, comprobar_nombre=True):
    """Contexto para Monitor, WM_W y Front: solo confía en la CA del proyecto.

    El certificado del servidor siempre se verifica. comprobar_nombre=False
    solo desactiva la comparación con el nombre o IP, para pruebas locales
    con un certificado emitido para otra dirección.
    """
    contexto = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    contexto.minimum_version = ssl.TLSVersion.TLSv1_2
    contexto.load_verify_locations(cafile=str(ca))
    contexto.check_hostname = comprobar_nombre
    contexto.verify_mode = ssl.CERT_REQUIRED
    return contexto

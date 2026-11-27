"""CA propia y certificado de servidor para el TLS de Release 2 (ARCHITECTURE.md D2).

Se usa `cryptography` y no el comando openssl para que funcione igual en Linux
(AWS), macOS y los Windows del laboratorio, y para que las pruebas generen sus
propios certificados con el mismo código.

Ficheros en la carpeta indicada (por defecto certs/, ignorada por Git):
  ca.pem          certificado de la CA: se copia a los PCs del laboratorio (es público)
  ca.key          clave privada de la CA: NUNCA sale de la máquina donde se generó
  servidor.pem    certificado de CENTRAL y Registry (con las IPs/DNS en el SAN)
  servidor.key    su clave privada: NUNCA se sube a Git ni a la imagen Docker
"""

from datetime import datetime, timedelta, timezone
import ipaddress
import os
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

DIAS_CA = 825
DIAS_SERVIDOR = 397  # máximo que aceptan los navegadores para un certificado de servidor
NOMBRES_LOCALES = ("localhost", "127.0.0.1")


def _nombre(texto):
    return x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Water Management SD 26/27"),
                      x509.NameAttribute(NameOID.COMMON_NAME, texto)])


def _alt(nombre):
    """IP o nombre DNS para el subjectAltName."""
    try:
        return x509.IPAddress(ipaddress.ip_address(nombre))
    except ValueError:
        return x509.DNSName(nombre)


def generar_ca(dias=DIAS_CA):
    clave = ec.generate_private_key(ec.SECP256R1())
    ahora = datetime.now(timezone.utc)
    nombre = _nombre("Water Management CA")
    certificado = (
        x509.CertificateBuilder()
        .subject_name(nombre).issuer_name(nombre)
        .public_key(clave.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(ahora - timedelta(minutes=5))
        .not_valid_after(ahora + timedelta(days=dias))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False,
                                     key_encipherment=False, data_encipherment=False,
                                     key_agreement=False, key_cert_sign=True, crl_sign=True,
                                     encipher_only=False, decipher_only=False), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(clave.public_key()),
                       critical=False)
        .sign(clave, hashes.SHA256())
    )
    return clave, certificado


def generar_servidor(clave_ca, certificado_ca, nombres, dias=DIAS_SERVIDOR):
    """Certificado de servidor firmado por la CA, válido para cada IP o DNS de nombres."""
    nombres = list(dict.fromkeys(nombres))  # sin duplicados, en orden
    if not nombres:
        raise ValueError("Indica al menos una IP o nombre DNS del servidor")
    clave = ec.generate_private_key(ec.SECP256R1())
    ahora = datetime.now(timezone.utc)
    certificado = (
        x509.CertificateBuilder()
        .subject_name(_nombre(nombres[0]))
        .issuer_name(certificado_ca.subject)
        .public_key(clave.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(ahora - timedelta(minutes=5))
        .not_valid_after(min(ahora + timedelta(days=dias), certificado_ca.not_valid_after_utc))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=True, content_commitment=False,
                                     key_encipherment=False, data_encipherment=False,
                                     key_agreement=False, key_cert_sign=False, crl_sign=False,
                                     encipher_only=False, decipher_only=False), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.SubjectAlternativeName([_alt(n) for n in nombres]), critical=False)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(clave.public_key()),
                       critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(clave_ca.public_key()),
                       critical=False)
        .sign(clave_ca, hashes.SHA256())
    )
    return clave, certificado


def _escribir_clave(ruta, clave):
    datos = clave.private_bytes(serialization.Encoding.PEM,
                                serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    # Se crea ya con permisos 0600 (en Windows el modo se ignora). os.open solo
    # aplica el modo al crear el fichero: si ya existía con otros permisos, se
    # corrigen antes de escribir la clave.
    descriptor = os.open(ruta, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    if hasattr(os, "fchmod"):
        os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "wb") as fichero:
        fichero.write(datos)


def _escribir_certificado(ruta, certificado):
    Path(ruta).write_bytes(certificado.public_bytes(serialization.Encoding.PEM))


def generar_en(carpeta, nombres, nueva_ca=False):
    """Crea (o reutiliza) la CA y emite un certificado de servidor nuevo.

    La CA se reutiliza si ya existe, para no tener que volver a copiar ca.pem a
    los PCs del laboratorio cada vez que cambia la IP del servidor.
    Devuelve un diccionario con las rutas generadas.
    """
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    rutas = {n: carpeta / f for n, f in (("ca", "ca.pem"), ("ca_key", "ca.key"),
                                         ("cert", "servidor.pem"), ("key", "servidor.key"))}
    if nueva_ca or not (rutas["ca"].exists() and rutas["ca_key"].exists()):
        clave_ca, certificado_ca = generar_ca()
        _escribir_clave(rutas["ca_key"], clave_ca)
        _escribir_certificado(rutas["ca"], certificado_ca)
    else:
        clave_ca = serialization.load_pem_private_key(rutas["ca_key"].read_bytes(), password=None)
        certificado_ca = x509.load_pem_x509_certificate(rutas["ca"].read_bytes())
    clave, certificado = generar_servidor(clave_ca, certificado_ca, nombres)
    _escribir_clave(rutas["key"], clave)
    _escribir_certificado(rutas["cert"], certificado)
    return rutas

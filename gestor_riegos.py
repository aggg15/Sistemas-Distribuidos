"""Ciclo de riego de CENTRAL: autorizar, seguir y cerrar cada riego.

No usa Kafka directamente: recibe los mensajes ya decodificados y publica con
la función publicar(topic, mensaje, clave) -> bool. Así se prueba sin broker.

Recorrido (docs/DISENO.md, apartado 4):
  SOLICITUD → validar → reservar la WS → INICIAR_RIEGO al Engine
  RIEGO_INICIADO → AUTORIZADO al FO → TELEMETRIA cada segundo
  RIEGO_FINALIZADO → RESUMEN al FO y fila cerrada en la BD
"""

from dataclasses import dataclass
import itertools
import logging
import sqlite3
import threading
import time

from common.topics import TOPIC_ESTADO_RED, TOPIC_NOTIFICACIONES, TOPIC_ORDENES

LOG = logging.getLogger("WM_Central")

PLAZO_AUTORIZACION_S = 5.0   # tiempo que se espera a que el Engine abra la válvula
CADUCIDAD_SOLICITUD_S = 30.0  # solicitudes más antiguas no se atienden
MOTIVOS_FIN = ("TIEMPO", "MANUAL", "FUGA", "BLOQUEO", "DESCONEXION")
ORIGENES = ("FO", "WS", "CENTRAL")


@dataclass
class RiegoEnCurso:
    ws_id: str
    origen: str
    fo_id: object        # None si no lo pidió un operario
    peticion_id: object
    duracion_s: int
    limite: float        # hasta cuándo se espera RIEGO_INICIADO
    riego_id: int = 0
    activo: bool = False  # False: reservado, esperando al Engine
    caudal_lpm: float = 0.0
    volumen_l: float = 0.0
    segundos: int = 0


class GestorRiegos:
    """Las decisiones se toman de una en una (RLock): una solicitud, un evento
    del Engine o una avería nunca se procesan a la vez para la misma estación."""

    def __init__(self, registro, bd, publicar, reloj=time.monotonic):
        self.registro = registro
        self.bd = bd
        self.publicar = publicar
        self.reloj = reloj
        self._lock = threading.RLock()
        self._peticiones_central = itertools.count(1)

    # --- Avisos -------------------------------------------------------------

    def _avisar(self, riego_o_datos, tipo, texto, **extra):
        """Muestra el paso en CENTRAL y, si lo pidió un operario, se lo envía."""
        datos = riego_o_datos
        if isinstance(riego_o_datos, RiegoEnCurso):
            datos = {"fo_id": datos.fo_id, "peticion_id": datos.peticion_id,
                     "ws_id": datos.ws_id}
        nivel = logging.WARNING if tipo == "DENEGADO" else logging.INFO
        if tipo != "TELEMETRIA":
            LOG.log(nivel, "[%s] %s: %s", datos["ws_id"], tipo, texto)
        if datos.get("fo_id") is None:
            return
        self.publicar(TOPIC_NOTIFICACIONES, {
            "tipo": tipo, "fo_id": datos["fo_id"], "peticion_id": datos["peticion_id"],
            "ws_id": datos["ws_id"], "texto": texto, **extra,
        }, datos["fo_id"])

    # --- Solicitudes (FO, menú de la WS o CENTRAL) ---------------------------

    def procesar_peticion(self, peticion, antiguedad_s=0.0):
        if not isinstance(peticion, dict) or peticion.get("tipo") != "SOLICITUD":
            LOG.warning("Petición ignorada: %s", peticion)
            return None
        origen = peticion.get("origen")
        if origen not in ORIGENES:
            LOG.warning("Petición con origen desconocido: %s", peticion)
            return None
        datos = {"fo_id": peticion.get("fo_id") if origen == "FO" else None,
                 "peticion_id": peticion.get("peticion_id"),
                 "ws_id": peticion.get("ws_id")}
        self._avisar(datos, "PASO", f"Solicitud de riego recibida (origen {origen})")

        if antiguedad_s > CADUCIDAD_SOLICITUD_S:
            # Llegó mientras CENTRAL estaba caída: ya no tiene sentido regar.
            self._avisar(datos, "DENEGADO", "Solicitud caducada")
            return None
        if origen == "FO" and not (datos["fo_id"] and self.bd.operario_existe(datos["fo_id"])):
            self._avisar(datos, "DENEGADO", "Operario no registrado en CENTRAL")
            return None
        try:
            duracion_s = int(peticion["duracion_s"])
            if duracion_s <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            self._avisar(datos, "DENEGADO", "Duración no válida")
            return None
        return self.iniciar(datos["ws_id"], duracion_s, origen, datos["fo_id"],
                            datos["peticion_id"])

    def iniciar(self, ws_id, duracion_s, origen="CENTRAL", fo_id=None, peticion_id=None):
        if peticion_id is None:
            peticion_id = f"CENTRAL-{next(self._peticiones_central)}"
        riego = RiegoEnCurso(ws_id, origen, fo_id, peticion_id, duracion_s,
                             limite=self.reloj() + PLAZO_AUTORIZACION_S)
        with self._lock:
            self._avisar(riego, "PASO", "Comprobando disponibilidad de la estación")
            motivo = self.registro.reservar(ws_id, riego)
            if motivo is not None:
                self._avisar(riego, "DENEGADO", motivo)
                return None
            try:
                riego.riego_id = self.bd.crear_riego(ws_id, fo_id, origen)
            except sqlite3.Error:
                LOG.exception("No se pudo guardar el riego")
                self.registro.liberar(ws_id, riego)
                self._avisar(riego, "DENEGADO", "Error en la base de datos de CENTRAL")
                return None
            self._avisar(riego, "PASO",
                         f"Estación disponible: solicitando autorización (riego {riego.riego_id})")
            enviada = self.publicar(TOPIC_ORDENES, {
                "tipo": "INICIAR_RIEGO", "ws_id": ws_id, "riego_id": riego.riego_id,
                "fo_id": fo_id, "duracion_s": duracion_s,
            }, ws_id)
            if not enviada:
                self._terminar(riego, None, texto="No se pudo enviar la orden a la estación")
                return None
            return riego.riego_id

    # --- Eventos del Engine --------------------------------------------------

    def procesar_evento(self, evento):
        if not isinstance(evento, dict):
            return
        ws_id, tipo = evento.get("ws_id"), evento.get("tipo")
        with self._lock:
            riego = self.registro.riego_de(ws_id)
            if riego is None or riego.riego_id != evento.get("riego_id"):
                # Riego ya cerrado por CENTRAL (avería, bloqueo...) o desconocido.
                LOG.debug("Evento %s de un riego que no está en curso: %s", tipo, evento)
                return
            if tipo == "RIEGO_INICIADO" and not riego.activo:
                riego.activo = True
                self.bd.iniciar_riego(riego.riego_id)
                self._avisar(riego, "AUTORIZADO",
                             f"Riego autorizado: electroválvula abierta durante "
                             f"{riego.duracion_s} s")
            elif tipo == "RIEGO_RECHAZADO":
                self._terminar(riego, None,
                               texto=f"La estación rechazó el riego ({evento.get('motivo')})")
            elif tipo == "TELEMETRIA":
                riego.caudal_lpm = evento.get("caudal_lpm", 0.0)
                riego.volumen_l = evento.get("volumen_l", 0.0)
                riego.segundos = evento.get("segundos", riego.segundos)
                self._avisar(riego, "TELEMETRIA", "", caudal_lpm=riego.caudal_lpm,
                             volumen_l=riego.volumen_l, segundos=riego.segundos)
            elif tipo == "RIEGO_FINALIZADO":
                self._terminar(riego, evento.get("motivo"), evento.get("volumen_l"),
                               evento.get("duracion_s"))

    # --- Cortes decididos por CENTRAL ----------------------------------------

    def interrumpir(self, ws_id, motivo):
        """Avería, desconexión o bloqueo: el riego termina ya, conteste o no el Engine."""
        with self._lock:
            riego = self.registro.riego_de(ws_id)
            if riego is None:
                return
            self.publicar(TOPIC_ORDENES, {"tipo": "PARAR", "ws_id": ws_id,
                                          "riego_id": riego.riego_id, "motivo": motivo}, ws_id)
            self._terminar(riego, motivo)

    def revisar_plazos(self):
        """Cancela las autorizaciones a las que el Engine no ha respondido a tiempo."""
        with self._lock:
            for ws_id in self.registro.ids():
                riego = self.registro.riego_de(ws_id)
                if riego is not None and not riego.activo and self.reloj() > riego.limite:
                    # Por si el Engine abre la válvula tarde: la orden PARAR llega después.
                    self.publicar(TOPIC_ORDENES, {"tipo": "PARAR", "ws_id": ws_id,
                                                  "riego_id": riego.riego_id,
                                                  "motivo": "MANUAL"}, ws_id)
                    self._terminar(riego, None, texto="La estación no ha respondido a tiempo")

    def _terminar(self, riego, motivo, volumen_l=None, duracion_s=None, texto=None):
        """Libera la estación. Si llegó a regar, cierra la fila y envía el resumen;
        si no, borra la fila y deniega la solicitud."""
        if self.registro.liberar(riego.ws_id, riego) is None:
            return  # Otro hilo ya lo cerró.
        if not riego.activo:
            if riego.riego_id:
                self.bd.borrar_riego(riego.riego_id)
            self._avisar(riego, "DENEGADO", texto or f"Riego cancelado ({motivo})")
            return
        motivo = motivo if motivo in MOTIVOS_FIN else "MANUAL"
        volumen_l = riego.volumen_l if volumen_l is None else volumen_l
        duracion_s = riego.segundos if duracion_s is None else duracion_s
        try:
            self.bd.cerrar_riego(riego.riego_id, duracion_s, volumen_l, motivo)
        except sqlite3.Error:
            LOG.exception("No se pudo cerrar el riego %s en la BD", riego.riego_id)
        self._avisar(riego, "RESUMEN",
                     f"Riego finalizado ({motivo}): {volumen_l:.2f} L en {duracion_s} s",
                     motivo=motivo, volumen_l=volumen_l, duracion_s=duracion_s)

    # --- Órdenes de CENTRAL (punto 11 del enunciado) -------------------------

    def ordenar(self, accion, ws_id=None, duracion_s=None):
        """accion: INICIAR, BLOQUEAR o ACTIVAR, sobre una estación o todas (ws_id=None)."""
        destinos = [ws_id] if ws_id is not None else self.registro.ids()
        for destino in destinos:
            if accion == "INICIAR":
                self.iniciar(destino, duracion_s, "CENTRAL")
            elif accion in ("BLOQUEAR", "ACTIVAR"):
                bloquear = accion == "BLOQUEAR"
                if not self.registro.bloquear(destino, bloquear):
                    LOG.warning("No existe la estación %s", destino)
                    continue
                LOG.info("[%s] %s por orden de CENTRAL", destino,
                         "FUERA DE SERVICIO" if bloquear else "ACTIVADA")
                self.publicar(TOPIC_ORDENES, {"tipo": accion, "ws_id": destino}, destino)
                if bloquear:
                    self.interrumpir(destino, "BLOQUEO")
            else:
                raise ValueError(f"Acción desconocida: {accion}")

    def publicar_estado_red(self):
        """Los operarios ven todas las estaciones y en qué estado están."""
        estaciones = [{"ws_id": ws_id, "ubicacion": datos["ubicacion"],
                       "estado": datos["estado"]}
                      for ws_id, datos in sorted(self.registro.estado().items())]
        self.publicar(TOPIC_ESTADO_RED, {"tipo": "ESTADO_RED", "estaciones": estaciones},
                      "red")

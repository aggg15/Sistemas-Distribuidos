"use strict";
// API REST de WM_Registry con Express (tutorial «Servicios web – API REST con
// Node.js», apartado 4). El CRUD de /usuarios del tutorial se convierte en el
// alta, consulta, modificación y baja de estaciones de riego:
//
//   VERBO   RUTA (prefijo /registry/v1)          DESCRIPCIÓN
//   GET     /estaciones                          Listado de estaciones (datos públicos)
//   GET     /estaciones/{ws_id}                  Datos de una estación (con su credencial)
//   PUT     /estaciones/{ws_id}                  Alta (devuelve la credencial) o cambio de ubicación
//   DELETE  /estaciones/{ws_id}                  Baja (con su credencial)
//   POST    /estaciones/{ws_id}/autenticacion    Comprobar la credencial
//
// La credencial viaja en la cabecera «Authorization: Bearer <credencial>» y solo
// se devuelve en claro una vez, en la respuesta del alta. Diferencias con el
// tutorial: errores con su código HTTP y JSON en vez de texto («No hay
// resultados») o `throw` (que tiraría el servidor), y consultas con parámetros.

const express = require("express");

const { EstacionYaRegistrada } = require("./bd");
const { generarCredencial, hashCredencial, verificarCredencial } = require("./credenciales");

const MAX_CUERPO = "4kb";
const MAX_ID = 32;
const MAX_UBICACION = 100;

/**
 * Mismo criterio que campo_valido() de common/configuracion.py: no vacío, sin
 * espacios al principio o al final, solo caracteres imprimibles y sin '#',
 * que separa los campos del protocolo de sockets.
 */
function campoValido(texto, maximo) {
  return typeof texto === "string" && texto !== "" && texto.length <= maximo
    && texto === texto.trim() && /^(?:[^\p{C}\p{Z}]| )+$/u.test(texto) && !texto.includes("#");
}

function ipDe(peticion) {
  // Se usa la IP de la conexión, no X-Forwarded-For (la podría inventar el cliente).
  const ip = peticion.socket.remoteAddress || "";
  return ip.startsWith("::ffff:") ? ip.slice(7) : ip;
}

function credencialDe(peticion) {
  const cabecera = peticion.get("authorization") || "";
  const coincidencia = /^Bearer ([A-Za-z0-9_-]+)$/.exec(cabecera);
  return coincidencia ? coincidencia[1] : null;
}

function error(respuesta, codigo, texto) {
  return respuesta.status(codigo).json({ error: texto });
}

function crearApp({ bd, log = console }) {
  const app = express();
  app.disable("x-powered-by");
  // El tutorial usa body-parser; Express lo incluye como express.json().
  app.use(express.json({ limit: MAX_CUERPO }));
  app.use((peticion, respuesta, siguiente) => {
    // Las respuestas pueden llevar una credencial: que nadie las guarde en caché.
    respuesta.set("Cache-Control", "no-store");
    siguiente();
  });

  function auditar(peticion, accion, resultado, wsId, detalle = {}) {
    try {
      bd.auditar({ accion, resultado, origenId: wsId, ip: ipDe(peticion), detalle });
    } catch (fallo) {
      // Un fallo de auditoría no debe impedir el servicio, pero se avisa.
      log.error(`[Registry] No se pudo guardar la auditoría ${accion}: ${fallo.message}`);
    }
  }

  // Normaliza y valida el ID de la ruta (en mayúsculas, como en Release 1).
  function idDeRuta(peticion, respuesta, siguiente) {
    const wsId = String(peticion.params.wsId).toUpperCase();
    if (!campoValido(wsId, MAX_ID)) {
      return error(respuesta, 400, "ID de estación no válido");
    }
    peticion.wsId = wsId;
    return siguiente();
  }

  // Comprueba «Authorization: Bearer <credencial>» contra el hash de la BD.
  async function autenticar(peticion, respuesta, siguiente) {
    const { wsId } = peticion;
    const estacion = bd.obtener(wsId);
    if (!estacion || estacion.estado_registro !== "ALTA") {
      return error(respuesta, 404, "La estación no está dada de alta");
    }
    const credencial = credencialDe(peticion);
    if (!credencial || !(await verificarCredencial(credencial, bd.hashCredencial(wsId)))) {
      auditar(peticion, "AUTH_KO", "KO", wsId,
        { servicio: "WM_Registry", motivo: credencial ? "CREDENCIAL_INVALIDA" : "SIN_CREDENCIAL" });
      log.info(`[Registry] Credencial rechazada para ${wsId} desde ${ipDe(peticion)}`);
      respuesta.set("WWW-Authenticate", 'Bearer realm="WM_Registry"');
      return error(respuesta, 401, "Credencial no válida");
    }
    peticion.estacion = estacion;
    return siguiente();
  }

  // Página de inicio, como el GET / del tutorial.
  app.get("/", (peticion, respuesta) => {
    respuesta.json({ servicio: "WM_Registry", api: "/registry/v1/estaciones" });
  });

  const rutas = express.Router();

  // Listado de todas las estaciones (tutorial 4.2).
  rutas.get("/estaciones", (peticion, respuesta) => {
    respuesta.json(bd.listar());
  });

  // Datos de una estación (tutorial 4.3). Solo la propia estación puede consultarlos.
  rutas.get("/estaciones/:wsId", idDeRuta, autenticar, (peticion, respuesta) => {
    respuesta.json(peticion.estacion);
  });

  // Alta o cambio de ubicación (tutorial 4.4 y 4.5). Se usa PUT y no POST
  // porque el cliente elige el identificador: repetir la petición no crea dos.
  rutas.put("/estaciones/:wsId", idDeRuta, async (peticion, respuesta) => {
    const { wsId } = peticion;
    const ubicacion = peticion.body && peticion.body.ubicacion;
    if (!campoValido(ubicacion, MAX_UBICACION)) {
      return error(respuesta, 400, 'Se esperaba un JSON con "ubicacion" válida');
    }
    if (bd.hashCredencial(wsId)) {
      // Ya está en ALTA con credencial: solo ella puede cambiar su ubicación.
      if (!credencialDe(peticion)) {
        auditar(peticion, "REGISTRY_KO", "KO", wsId, { motivo: "YA_REGISTRADA" });
        return error(respuesta, 409,
          "La estación ya está dada de alta; para cambiar la ubicación, envía su credencial");
      }
      return autenticar(peticion, respuesta, () => {
        const estacion = bd.cambiarUbicacion(wsId, ubicacion);
        auditar(peticion, "REGISTRY_MODIFICACION", "OK", wsId, { ubicacion });
        log.info(`[Registry] Ubicación de ${wsId} cambiada a «${ubicacion}»`);
        respuesta.json(estacion);
      });
    }
    const credencial = generarCredencial();
    const hash = await hashCredencial(credencial);
    try {
      const { estacion, realta } = bd.alta(wsId, ubicacion, hash);
      auditar(peticion, "REGISTRY_ALTA", "OK", wsId, { ubicacion, realta });
      log.info(`[Registry] Alta de ${wsId} («${ubicacion}») desde ${ipDe(peticion)}`);
      return respuesta.status(201).location(`/registry/v1/estaciones/${encodeURIComponent(wsId)}`)
        .json({ ...estacion, credencial });
    } catch (fallo) {
      if (fallo instanceof EstacionYaRegistrada) {
        // Otra alta del mismo ID ganó la carrera entre la comprobación y la escritura.
        auditar(peticion, "REGISTRY_KO", "KO", wsId, { motivo: "YA_REGISTRADA" });
        return error(respuesta, 409, "La estación ya está dada de alta");
      }
      throw fallo;
    }
  });

  // Baja (tutorial 4.6). Es lógica: se conserva el historial de riegos.
  rutas.delete("/estaciones/:wsId", idDeRuta, autenticar, (peticion, respuesta) => {
    const { wsId } = peticion;
    bd.baja(wsId);
    auditar(peticion, "REGISTRY_BAJA", "OK", wsId);
    log.info(`[Registry] Baja de ${wsId} desde ${ipDe(peticion)}`);
    respuesta.status(204).end();
  });

  // Opción de autenticación del Registry (p. 3 del enunciado).
  rutas.post("/estaciones/:wsId/autenticacion", idDeRuta, autenticar, (peticion, respuesta) => {
    auditar(peticion, "AUTH_OK", "OK", peticion.wsId, { servicio: "WM_Registry" });
    respuesta.json({ ws_id: peticion.wsId, autenticada: true });
  });

  app.use("/registry/v1", rutas);

  app.use((peticion, respuesta) => error(respuesta, 404, "No encontrado"));

  // Errores: JSON con el código adecuado en lugar de tirar el servidor.
  app.use((fallo, peticion, respuesta, siguiente) => {
    if (respuesta.headersSent) return siguiente(fallo);
    if (fallo.type === "entity.too.large") return error(respuesta, 413, "Cuerpo demasiado grande");
    if (fallo.type === "entity.parse.failed") return error(respuesta, 400, "JSON no válido");
    if (/locked|busy/i.test(fallo.message || "")) {
      return error(respuesta, 503, "Base de datos ocupada, reintenta en unos segundos");
    }
    log.error(`[Registry] Error inesperado: ${fallo.stack || fallo}`);
    return error(respuesta, 500, "Error interno del Registry");
  });

  return app;
}

module.exports = { crearApp, campoValido };

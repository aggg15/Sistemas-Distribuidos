"use strict";
// Acceso de WM_Registry a la BD SQLite que comparte con WM_Central (data/wm.db).
//
// El tutorial usa MySQL con el paquete `mysql`. La práctica exige SQLite en la
// nube (p. 9 del enunciado) y CENTRAL ya la usa, así que aquí se usa el módulo
// node:sqlite que trae Node.js (sin dependencias nativas que compilar).
//
// El esquema lo crea y lo migra common/base_datos.py (Python). Este módulo solo
// comprueba que exista: así no hay dos definiciones del esquema que puedan
// dejar de coincidir.
//
// Todas las consultas usan parámetros (?), nunca texto concatenado: el ejemplo
// del tutorial (`WHERE idUsuario = ${id}`) permitiría inyección SQL.

const fs = require("node:fs");
const { DatabaseSync } = require("node:sqlite");

const COLUMNAS_PUBLICAS = "id, ubicacion, estado_registro, fecha_alta, fecha_baja";
const COLUMNAS_NECESARIAS = ["estado_registro", "credencial_hash", "fecha_alta", "fecha_baja",
  "clave_cifrado", "clave_emitida"];
const ESPERA_BLOQUEO_MS = 1000; // mismo plazo que las conexiones de common/base_datos.py

class EsquemaNoPreparado extends Error {}
class EstacionYaRegistrada extends Error {}

/** Mismo formato que _ahora() de common/base_datos.py: ISO 8601 en UTC, sin milisegundos. */
function ahora() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "+00:00");
}

function publica(fila) {
  if (!fila) return null;
  return {
    ws_id: fila.id,
    ubicacion: fila.ubicacion,
    estado_registro: fila.estado_registro,
    fecha_alta: fila.fecha_alta,
    fecha_baja: fila.fecha_baja,
  };
}

class BaseDatosRegistry {
  /** Abre la BD. Lanza EsquemaNoPreparado si CENTRAL aún no la ha creado o migrado. */
  constructor(ruta) {
    if (!fs.existsSync(ruta)) {
      throw new EsquemaNoPreparado(`No existe la base de datos ${ruta}`);
    }
    this.bd = new DatabaseSync(ruta);
    try {
      this.bd.exec(`PRAGMA busy_timeout = ${ESPERA_BLOQUEO_MS}`);
      this.bd.exec("PRAGMA foreign_keys = ON");
      this.comprobarEsquema();
    } catch (error) {
      this.bd.close();
      throw error;
    }
  }

  comprobarEsquema() {
    const columnas = new Set(this.bd.prepare("PRAGMA table_info(estaciones)").all()
      .map((fila) => fila.name));
    const faltan = COLUMNAS_NECESARIAS.filter((columna) => !columnas.has(columna));
    const auditoria = this.bd.prepare(
      "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'auditoria'").get();
    if (columnas.size === 0 || faltan.length > 0 || !auditoria) {
      throw new EsquemaNoPreparado("La BD no tiene aún el esquema de Release 2");
    }
  }

  cerrar() {
    if (!this.cerrada) {
      this.bd.close();
      this.cerrada = true;
    }
  }

  // --- Consultas (GET) ------------------------------------------------------

  listar() {
    return this.bd.prepare(`SELECT ${COLUMNAS_PUBLICAS} FROM estaciones ORDER BY id`)
      .all().map(publica);
  }

  /** Datos públicos de una estación, o null. */
  obtener(wsId) {
    return publica(this.bd.prepare(`SELECT ${COLUMNAS_PUBLICAS} FROM estaciones WHERE id = ?`)
      .get(wsId));
  }

  /** Solo para comprobar credenciales: incluye el hash, nunca se envía al cliente. */
  hashCredencial(wsId) {
    const fila = this.bd.prepare(
      "SELECT estado_registro, credencial_hash FROM estaciones WHERE id = ?").get(wsId);
    return fila && fila.estado_registro === "ALTA" ? fila.credencial_hash : null;
  }

  // --- Alta, modificación y baja (PUT, DELETE) ------------------------------

  _transaccion(funcion) {
    // BEGIN IMMEDIATE: CENTRAL (otro proceso) no puede escribir entre la
    // comprobación y la escritura.
    this.bd.exec("BEGIN IMMEDIATE");
    try {
      const resultado = funcion();
      this.bd.exec("COMMIT");
      return resultado;
    } catch (error) {
      try {
        this.bd.exec("ROLLBACK");
      } catch {
        // SQLite ya deshizo la transacción (por ejemplo, tras un error de bloqueo).
      }
      throw error;
    }
  }

  /**
   * Alta de una estación. Misma regla que BaseDatos.alta_registry (Python):
   * se admite la re-alta de una estación en BAJA y la de una fila de Release 1
   * sin credencial; si ya está en ALTA con credencial, lanza EstacionYaRegistrada.
   * Devuelve { estacion, realta }.
   */
  alta(wsId, ubicacion, credencialHash) {
    const instante = ahora();
    return this._transaccion(() => {
      const fila = this.bd.prepare(
        "SELECT estado_registro, credencial_hash FROM estaciones WHERE id = ?").get(wsId);
      if (!fila) {
        this.bd.prepare(
          `INSERT INTO estaciones (id, ubicacion, ultima_conexion, estado_registro,
                                   credencial_hash, fecha_alta)
           VALUES (?, ?, ?, 'ALTA', ?, ?)`).run(wsId, ubicacion, instante, credencialHash, instante);
      } else if (fila.estado_registro === "ALTA" && fila.credencial_hash !== null) {
        throw new EstacionYaRegistrada(wsId);
      } else {
        // Se conservan bloqueo e historial de riegos; la clave anterior deja de valer.
        this.bd.prepare(
          `UPDATE estaciones SET ubicacion = ?, estado_registro = 'ALTA', credencial_hash = ?,
                  fecha_alta = ?, fecha_baja = NULL, clave_cifrado = NULL, clave_emitida = NULL
           WHERE id = ?`).run(ubicacion, credencialHash, instante, wsId);
      }
      return { estacion: this.obtener(wsId), realta: Boolean(fila) };
    });
  }

  /** Cambio de ubicación de una estación en ALTA (PUT con su credencial). */
  cambiarUbicacion(wsId, ubicacion) {
    const resultado = this.bd.prepare(
      "UPDATE estaciones SET ubicacion = ? WHERE id = ? AND estado_registro = 'ALTA'")
      .run(ubicacion, wsId);
    return resultado.changes > 0 ? this.obtener(wsId) : null;
  }

  /** Baja lógica: la fila se conserva porque riegos la referencia. Borra credencial y clave. */
  baja(wsId) {
    const resultado = this.bd.prepare(
      `UPDATE estaciones SET estado_registro = 'BAJA', fecha_baja = ?, credencial_hash = NULL,
              clave_cifrado = NULL, clave_emitida = NULL
       WHERE id = ? AND estado_registro = 'ALTA'`).run(ahora(), wsId);
    return resultado.changes > 0;
  }

  // --- Auditoría (tabla creada en #1) ---------------------------------------

  auditar({ accion, resultado, origenId = null, ip = null, detalle = {} }) {
    this.bd.prepare(
      `INSERT INTO auditoria (fecha, origen_tipo, origen_id, ip, accion, resultado, detalle)
       VALUES (?, 'WS', ?, ?, ?, ?, ?)`)
      .run(ahora(), origenId, ip, accion, resultado, JSON.stringify(detalle));
  }
}

module.exports = { BaseDatosRegistry, EsquemaNoPreparado, EstacionYaRegistrada, ahora };

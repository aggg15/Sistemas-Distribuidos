"use strict";
// WM_Front: aplicación web pública con el cuadro de monitorización de CENTRAL.
//
//   npm start                     (dentro de WM_Front/)
//   node WM_Front.js --puerto 8000 --api http://IP_CENTRAL:8080
//
// Variables de entorno (los argumentos tienen prioridad):
//   WM_FRONT_PORT        puerto HTTP del Front (8000)
//   WM_API_CENTRAL_URL   URL de API_Central (http://localhost:8080)

const { parseArgs } = require("node:util");

const { crearApp } = require("./app");

const AYUDA = `Uso: node WM_Front.js [opciones]

  --puerto N    puerto HTTP (WM_FRONT_PORT, 8000; 0 = uno libre)
  --host IP     interfaz de escucha (WM_FRONT_HOST, 0.0.0.0)
  --api URL     URL de API_Central (WM_API_CENTRAL_URL, http://localhost:8080)
  -h, --help    esta ayuda
`;

function leerConfiguracion(argv = process.argv.slice(2), entorno = process.env) {
  const { values } = parseArgs({
    args: argv,
    options: {
      puerto: { type: "string" },
      host: { type: "string" },
      api: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  const puerto = Number(values.puerto ?? entorno.WM_FRONT_PORT ?? 8000);
  if (!Number.isInteger(puerto) || puerto < 0 || puerto > 65535) {
    throw new Error("El puerto debe ser un entero entre 0 y 65535");
  }
  const api = values.api ?? entorno.WM_API_CENTRAL_URL ?? "http://localhost:8080";
  if (!/^https?:\/\//.test(api)) {
    throw new Error("La URL de API_Central debe empezar por http:// o https://");
  }
  return {
    ayuda: Boolean(values.help),
    puerto,
    host: values.host ?? entorno.WM_FRONT_HOST ?? "0.0.0.0",
    api,
  };
}

function arrancar(config, log = console) {
  const app = crearApp({ urlApi: config.api, log });
  return new Promise((resolver, rechazar) => {
    const servidor = app.listen(config.puerto, config.host, (fallo) => {
      if (fallo) return rechazar(fallo);
      log.info(`[Front] WM_Front escuchando en el puerto ${servidor.address().port}`
        + ` (API_Central: ${config.api})`);
      return resolver(servidor);
    });
  });
}

async function main() {
  let config;
  try {
    config = leerConfiguracion();
  } catch (fallo) {
    console.error(`${fallo.message}\n\n${AYUDA}`);
    process.exit(2);
  }
  if (config.ayuda) {
    console.log(AYUDA);
    return;
  }
  try {
    const servidor = await arrancar(config);
    const parar = () => {
      console.info("[Front] Parando WM_Front...");
      servidor.close(() => process.exit(0));
      servidor.closeAllConnections();
    };
    process.on("SIGINT", parar);
    process.on("SIGTERM", parar);
  } catch (fallo) {
    console.error(`[Front] No se pudo arrancar: ${fallo.message}`);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { arrancar, leerConfiguracion };

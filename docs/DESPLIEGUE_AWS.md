# Despliegue en AWS (CI/CD)

Flujo que pide la asignatura: **VS Code → GitHub → AWS**. Sigue el tutorial
"Introducción a AWS-Labs en configuración CI/CD" con una diferencia, explicada
en el apartado 1.

```text
 VS Code ── git push ──▶ GitHub ── Actions: imagen ──────────▶ Docker Hub
                                                                     │ docker compose pull
                                                                     ▼
                AWS (instancia EC2 "wm-central", Docker)
         ┌─────────────────────────────────────┐
         │  central (WM_Central + SQLite)      │◀── sockets :9999 ── WM_WS_M (lab)
         │      │ kafka:29092 (red interna)    │
         │  kafka ─────────────────────────────│◀── Kafka :9092 ──── WM_WS_E, WM_FO (lab)
         └─────────────────────────────────────┘
```

## 1. Por qué CENTRAL va en EC2 y no en ECS Express

El tutorial publica la aplicación web en **ECS Express**, que solo ofrece una URL
**HTTP** detrás de un balanceador. CENTRAL no es solo una web:

- Los Monitores se conectan por **sockets TCP** (puerto 9999), que un
  balanceador HTTP no deja pasar.
- La base de datos **SQLite** es un fichero. En ECS Express el disco del
  contenedor se pierde en cada despliegue; en EC2 vive en un volumen de Docker
  que se conserva.

Por eso CENTRAL se ejecuta en la misma instancia EC2 que Kafka, con la imagen
que GitHub Actions publica en Docker Hub. El resto del flujo CI/CD es el del
tutorial.

## 2. Docker Hub y secretos de GitHub (una vez)

1. En [hub.docker.com](https://hub.docker.com), crear el repositorio
   **`practica-aws`**, visibilidad **Public**.
2. En Docker Hub, **Account settings → Personal access tokens → Generate new
   token** con permiso *Read & Write*. Es preferible a usar la contraseña.
3. En GitHub, en el repositorio: **Settings → Secrets and variables → Actions →
   New repository secret**:
   - `DOCKER_USERNAME`: el usuario de Docker Hub.
   - `DOCKER_PASSWORD`: el token del paso anterior.

El fichero [.github/workflows/deploy.yml](../.github/workflows/deploy.yml) se
es el del tutorial: en cada `git push` a `main` construye la imagen con el
`Dockerfile` y la sube como `usuario/practica-aws:latest`. El resultado se ve
en la pestaña **Actions** de GitHub. Las pruebas se lanzan antes en local con
`python -m unittest`.

## 3. Crear la instancia EC2

En AWS Academy: **Start Lab**, esperar al círculo verde y pulsar **AWS**. En la
consola: **EC2 → Lanzar instancia**.

| Campo | Valor |
| --- | --- |
| Nombre | `wm-central` |
| AMI | Ubuntu (24.04 LTS) |
| Tipo de instancia | `t3.small` o `t3.medium` (ver nota) |
| Par de claves | Proceder sin un par de claves (se entra desde el navegador) |
| Red | Grupo de seguridad existente `default` |
| Almacenamiento | 16 GiB |

> **Tipo de instancia**: el tutorial usa `t2.micro` (1 GB de memoria), pero ahí
> solo corre Kafka. Aquí corren Kafka y CENTRAL juntos y 1 GB se queda corto. Si
> el laboratorio solo permite `t2.micro`, crear dos instancias: una para Kafka
> (como en el tutorial) y otra para CENTRAL, y arrancar CENTRAL con
> `WM_KAFKA_SERVER=IP_KAFKA:9092`.

### Reglas del grupo de seguridad

**EC2 → Grupos de seguridad → default → Editar reglas de entrada**:

| Tipo | Puerto | Origen | Para qué |
| --- | --- | --- | --- |
| SSH | 22 | 0.0.0.0/0 | Terminal desde el navegador (EC2 Instance Connect) |
| TCP personalizado | 9999 | 0.0.0.0/0 | Sockets de los Monitores con CENTRAL |
| TCP personalizado | 9092 | 0.0.0.0/0 | Kafka para Engines y operarios |
| TCP personalizado | 8080 | 0.0.0.0/0 | Panel web de CENTRAL (`http://IP_AWS:8080`) |

> **Seguridad**: como indica el tutorial, en producción Kafka iría en una subred
> privada. Aquí no hay autenticación y los puertos quedan abiertos a Internet.
> Si se conoce la IP pública del laboratorio, conviene ponerla como origen de
> 9999 y 9092 en lugar de `0.0.0.0/0`.

### IP fija (Elastic IP), recomendable

Al parar el laboratorio, AWS detiene la instancia y al volver a arrancarla
**cambia la IP pública**: Kafka anunciaría una IP vieja y habría que cambiar los
comandos del laboratorio. Una Elastic IP lo evita:

1. **EC2 → Direcciones IP elásticas → Asignar** dirección.
2. **Acciones → Asociar** a la instancia `wm-central`.

En el resto de la guía, `IP_AWS` es esa dirección (o la IPv4 pública si no se
usa Elastic IP).

## 4. Preparar la instancia (una vez)

**Instancias → wm-central → Conectar → EC2 Instance Connect → Conectar**. En la
terminal del navegador:

```bash
sudo apt update -y
sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker ubuntu
```

Cerrar la pestaña y volver a conectar para que `usermod` tenga efecto (permite
usar Docker sin `sudo`). Después:

```bash
git clone https://github.com/aggg15/Sistemas-Distribuidos.git
cd Sistemas-Distribuidos
echo "WM_CENTRAL_IMAGE=USUARIO_DOCKERHUB/practica-aws:latest" > .env
```

El fichero `.env` indica qué imagen de Docker Hub usar. Está en `.gitignore`: es
configuración de esta máquina y no se sube a GitHub.

## 5. Arrancar o actualizar

```bash
cd Sistemas-Distribuidos
bash deploy/arrancar_aws.sh
```

El script:

1. Averigua la IP pública de la instancia (o usa la que se le pase como
   argumento: `bash deploy/arrancar_aws.sh IP_AWS`).
2. Hace `git pull` para tener el `docker-compose.yml` al día.
3. Descarga de Docker Hub la última imagen de CENTRAL.
4. Arranca CENTRAL y Kafka. Kafka anuncia la IP pública a los clientes.

Es el equivalente al "Forzar nueva implementación" de ECS del tutorial: tras
cada `git push`, cuando la pestaña Actions esté en verde, se vuelve a ejecutar.

Ver los mensajes de CENTRAL (Ctrl+C deja de mostrarlos, CENTRAL sigue en marcha):

```bash
docker compose logs -f central
```

Los contenedores tienen `restart: unless-stopped`: si el laboratorio se para y
se vuelve a arrancar, se levantan solos. Sin Elastic IP, hay que volver a
ejecutar el script para que Kafka anuncie la IP nueva.

## 5 bis. Panel web en ECS Express (opcional)

Para mantener el paso 3 del tutorial (la web en ECS Express con su URL
pública), ECS ejecuta la misma imagen de CENTRAL en **modo panel remoto**: no
abre sockets ni se conecta a Kafka, solo sirve la página del panel y reenvía
sus consultas al panel de la CENTRAL de EC2
([WM_Central/panel_remoto.py](../WM_Central/panel_remoto.py)). Así la URL de
ECS muestra en todo momento lo mismo que `http://IP_AWS:8080`.

En **ECS → Modo exprés → Crear** (o **Actualizar** el servicio existente):

| Campo | Valor |
| --- | --- |
| URI de imagen | `docker.io/USUARIO_DOCKERHUB/practica-aws:latest` |
| Puerto del contenedor | `8080` |
| Variable de entorno | `WM_CENTRAL_REMOTA` = `http://IP_AWS:8080` |

Con esa variable la imagen arranca solo el panel. Sin ella es la CENTRAL
completa, la que corre en EC2. `IP_AWS` debe ser fija (Elastic IP): si
cambia, hay que actualizar la variable.

Tras cada `git push`, cuando Actions haya subido la imagen nueva, se actualizan
las dos partes: EC2 con `bash deploy/arrancar_aws.sh` y ECS con **Actualizar →
Forzar nueva implementación**, como en el tutorial.

## 6. Dar de alta a los operarios

Las solicitudes de un operario que no está en la base de datos se deniegan:

```bash
docker compose exec central python scripts/gestionar_bd.py operario FO1 "Ana"
docker compose exec central python scripts/gestionar_bd.py operarios
```

La base de datos está en el volumen `wm-datos`: se conserva al actualizar o
reiniciar el contenedor.

## 7. Conectar los módulos del laboratorio

Los módulos del laboratorio se lanzan con Docker, con la misma imagen que
publica GitHub Actions: no hace falta clonar el repositorio ni instalar Python
ni kafka-python en los PCs. Primero, en cada PC:

```bash
docker pull USUARIO_DOCKERHUB/practica-aws:latest
```

Cada módulo, en su propia terminal (`-it` para poder usar su menú):

```bash
docker run --rm -it --network host --name ws04-monitor USUARIO_DOCKERHUB/practica-aws python WM_WS/WM_WS_M.py IP_AWS 9999 WS-04 "River Park" --puerto-engine 9100
docker run --rm -it --network host --name ws04-engine -v wm-engine:/app/data USUARIO_DOCKERHUB/practica-aws python WM_WS/WM_WS_E.py IP_AWS:9092 localhost 9100
docker run --rm -it --network host --name fo1 USUARIO_DOCKERHUB/practica-aws python WM_FO/WM_FO.py IP_AWS:9092 FO1
```

- `--network host`: el contenedor usa la red del PC. Así el Engine llega a su
  Monitor por `localhost`, como sin Docker.
- `-v wm-engine:/app/data`: el Engine guarda ahí el riego en curso; si se cae y
  se vuelve a lanzar, lo recupera.
- `--name`: permite simular un *crash* desde otra terminal, por ejemplo
  `docker kill ws04-engine`.

Monitor y Engine de una misma estación van en el mismo PC. Cada estación
necesita su propio ID, su propio puerto de Engine y nombres de contenedor
distintos (`ws05-monitor`, `ws05-engine`, puerto 9101...).

## 8. Problemas habituales

| Síntoma | Causa probable |
| --- | --- |
| Actions en rojo | Secretos de Docker Hub mal escritos o token sin permiso de escritura. |
| `pull access denied` en la instancia | `WM_CENTRAL_IMAGE` mal escrita o repositorio de Docker Hub privado. |
| La URL de ECS muestra "Sin conexión con WM_Central" | `WM_CENTRAL_REMOTA` mal escrita, falta la regla del puerto 8080 o la IP de EC2 ha cambiado. |
| La URL de ECS muestra un panel vacío con "Sin Kafka" | Falta la variable `WM_CENTRAL_REMOTA`: ECS está ejecutando una CENTRAL completa aparte. |
| El Monitor no conecta con CENTRAL | Falta la regla del puerto 9999, o CENTRAL no está en marcha (`docker compose ps`). |
| El FO se queda en "Conectando con Kafka" | Falta la regla del puerto 9092. |
| El FO conecta pero luego da error de Kafka | Kafka anuncia una IP antigua: `bash deploy/arrancar_aws.sh IP_AWS`. |
| Kafka se reinicia continuamente | Poca memoria: usar `t3.small`/`t3.medium` o separar Kafka en otra instancia. |

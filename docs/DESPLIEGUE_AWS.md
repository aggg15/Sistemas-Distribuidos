# Despliegue en AWS (CI/CD)

Flujo que pide la asignatura: **VS Code → GitHub → AWS**. Sigue el tutorial
"Introducción a AWS-Labs en configuración CI/CD" con una diferencia, explicada
en el apartado 1.

```text
 VS Code ── git push ──▶ GitHub ── Actions: pruebas + imagen ──▶ Docker Hub
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
   **`wm-central`**, visibilidad **Public**.
2. En Docker Hub, **Account settings → Personal access tokens → Generate new
   token** con permiso *Read & Write*. Es preferible a usar la contraseña.
3. En GitHub, en el repositorio: **Settings → Secrets and variables → Actions →
   New repository secret**:
   - `DOCKER_USERNAME`: el usuario de Docker Hub.
   - `DOCKER_PASSWORD`: el token del paso anterior.

El fichero [.github/workflows/deploy.yml](../.github/workflows/deploy.yml) se
ejecuta en cada `git push` a `main`: lanza las pruebas y, solo si pasan,
construye la imagen y la sube como `usuario/wm-central:latest` (y con el hash
del commit). El resultado se ve en la pestaña **Actions** de GitHub.

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
echo "WM_CENTRAL_IMAGE=USUARIO_DOCKERHUB/wm-central:latest" > .env
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

## 6. Dar de alta a los operarios

Las solicitudes de un operario que no está en la base de datos se deniegan:

```bash
docker compose exec central python scripts/gestionar_bd.py operario FO1 "Ana"
docker compose exec central python scripts/gestionar_bd.py operarios
```

La base de datos está en el volumen `wm-datos`: se conserva al actualizar o
reiniciar el contenedor.

## 7. Conectar los módulos del laboratorio

En cada PC del laboratorio, con el repositorio clonado y las dependencias
instaladas (`pip install -r requirements.txt`):

```bash
python3 WM_WS_M.py IP_AWS 9999 WS-04 "River Park" --puerto-engine 9100
python3 WM_WS_E.py IP_AWS:9092 localhost 9100
python3 WM_FO.py IP_AWS:9092 FO1 --fichero peticiones_ejemplo.txt
```

Monitor y Engine de una misma estación van en el mismo PC (se hablan por
`localhost`). Cada estación necesita su propio ID y su propio puerto de Engine.

## 8. Problemas habituales

| Síntoma | Causa probable |
| --- | --- |
| Actions en rojo en "Pruebas" | Alguna prueba falla: no se publica imagen hasta arreglarla. |
| Actions en rojo en "Imagen" | Secretos de Docker Hub mal escritos o token sin permiso de escritura. |
| `pull access denied` en la instancia | `WM_CENTRAL_IMAGE` mal escrita o repositorio de Docker Hub privado. |
| El Monitor no conecta con CENTRAL | Falta la regla del puerto 9999, o CENTRAL no está en marcha (`docker compose ps`). |
| El FO se queda en "Conectando con Kafka" | Falta la regla del puerto 9092. |
| El FO conecta pero luego da error de Kafka | Kafka anuncia una IP antigua: `bash deploy/arrancar_aws.sh IP_AWS`. |
| Kafka se reinicia continuamente | Poca memoria: usar `t3.small`/`t3.medium` o separar Kafka en otra instancia. |

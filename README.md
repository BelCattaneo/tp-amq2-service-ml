# Ejemplo de ambiente productivo
### MLOps1 - CEIA - FIUBA
Estructura de servicios para la implementación del proyecto final de MLOps1 - CEIA - FIUBA

> **Nota:** Las consignas del trabajo práctico se encuentran en [CONSIGNAS.md](CONSIGNAS.md)

Supongamos que trabajamos para **ML Models and something more Inc.**, la cual ofrece un servicio que proporciona modelos mediante una REST API. Internamente, tanto para realizar tareas de DataOps como de MLOps, la empresa cuenta con varios servicios que ayudan a ejecutar las acciones necesarias. También dispone de un Data Lake en S3, para este caso, simularemos un S3 utilizando MinIO.

Para simular esta empresa, utilizaremos Docker y, a través de Docker Compose, desplegaremos varios contenedores que representan distintos servicios en un entorno productivo.

Los servicios que contamos son:
- [Apache Airflow](https://airflow.apache.org/)
- [MLflow](https://mlflow.org/)
- API Rest para servir modelos ([FastAPI](https://fastapi.tiangolo.com/))
- [MinIO](https://min.io/)
- Base de datos relacional [PostgreSQL](https://www.postgresql.org/)
- Base de dato key-value [ValKey](https://valkey.io/) 

![Diagrama de servicios](final_assign.png)

Por defecto, cuando se inician los multi-contenedores, se crean los siguientes buckets:

- `s3://data`
- `s3://mlflow` (usada por MLflow para guardar los artefactos).

y las siguientes bases de datos:

- `mlflow_db` (usada por MLflow).
- `airflow` (usada por Airflow).

## Instalación

1. Para poder levantar todos los servicios, primero instala [Docker](https://docs.docker.com/engine/install/) en tu computadora (o en el servidor que desees usar).
2. Clona este repositorio.
3. Crea las carpetas `airflow/config`, `airflow/dags`, `airflow/logs`, `airflow/plugins`.
4. Si estás en Linux o MacOS, en el archivo `.env`, reemplaza `AIRFLOW_UID` por el de tu usuario o alguno que consideres oportuno (para encontrar el UID, usa el comando `id -u <username>`). De lo contrario, Airflow dejará sus carpetas internas como root y no podrás subir DAGs (en `airflow/dags`) o plugins, etc.
5. En la carpeta raíz de este repositorio, ejecuta:

```bash
make install && make up
```

O usando docker-compose directamente:

```bash
docker compose --profile all up
```

6. Una vez que todos los servicios estén funcionando (verifica con el comando `docker ps -a` que todos los servicios estén healthy o revisa en Docker Desktop), podrás acceder a los diferentes servicios mediante:
   - Apache Airflow: http://localhost:8080
   - MLflow: http://localhost:5001
   - MinIO: http://localhost:9001 (ventana de administración de Buckets)
   - API: http://localhost:8800/
   - Documentación de la API: http://localhost:8800/docs

Si estás usando un servidor externo a tu computadora de trabajo, reemplaza `localhost` por su IP (puede ser una privada si tu servidor está en tu LAN o una IP pública si no; revisa firewalls u otras reglas que eviten las conexiones).

Todos los puertos u otras configuraciones se pueden modificar en el archivo `.env`. Se invita a jugar y romper para aprender; siempre puedes volver a clonar este repositorio.

## Comandos disponibles (Makefile)

El proyecto incluye un Makefile con comandos útiles para gestionar los servicios:

```bash
make help      # Muestra todos los comandos disponibles
make up        # Inicia todos los servicios
make down      # Detiene todos los servicios
make restart   # Reinicia todos los servicios
make install   # Reconstruye contenedores con nuevas dependencias
make clean     # Detiene y elimina todo (contenedores, redes y volúmenes)
make logs      # Muestra logs de todos los servicios en tiempo real
make status    # Muestra el estado de todos los servicios
```

### Flujo de trabajo común

**Primera vez:**
```bash
make install && make up
```

**Después de agregar dependencias en `requirements.txt`:**
```bash
make install
```

**Reiniciar desde cero:**
```bash
make clean
make install && make up
```

## ETL Pipeline: Chicago Crime Data

El proyecto incluye un pipeline ETL completo para análisis de crímenes en Chicago:

### Arquitectura del Pipeline

**Task 1: setup_s3**
- Crea bucket MinIO si no existe
- Configura política de lifecycle (TTL de 60 días para datos temporales)

**Task 2: download_data**
- Descarga datos de crímenes desde Socrata API (Chicago Data Portal)
- Descarga datos de estaciones policiales
- Primera ejecución: descarga año completo (~260k registros)
- Ejecuciones subsecuentes: descarga incremental mensual (~25k registros)
- Guarda en MinIO: `raw-data/crimes_YYYY-MM-DD.csv` y `raw-data/police_stations.csv`

**Task 3: enrich_data**
- Carga datos desde MinIO
- Calcula distancia a estación policial más cercana (GeoPandas spatial join)
- Crea features temporales:
  - Season (Winter/Spring/Summer/Fall)
  - Day of week (0-6)
  - Day time (Morning/Afternoon/Evening/Night)
- Guarda en MinIO: `raw-data/crimes_enriched_YYYY-MM-DD.csv`

**Tasks 4-8:** (Por implementar)
- `split_data` - División train/test (80/20 estratificado)
- `process_outliers` - Manejo de outliers y transformación logarítmica
- `encode_data` - Encoding de variables categóricas
- `scale_data` - Escalado con StandardScaler
- `balance_data` - Balanceo con SMOTE + RandomUnderSampler
- `extract_features` - Selección de features con Mutual Information

### Estructura de Módulos

```
airflow/dags/
├── etl_process_taskflow.py       # DAG principal con TaskFlow API
└── etl_helpers/
    ├── __init__.py               # Package initialization
    ├── minio_utils.py            # Operaciones MinIO/S3
    ├── data_loader.py            # Descarga desde Socrata API
    └── data_enrichment.py        # Enriquecimiento geoespacial y temporal
```

### Configuración

**Variables de entorno requeridas (`.env`):**
```bash
SOCRATA_APP_TOKEN=tu_token_aqui  # Token de Socrata API
DATA_REPO_BUCKET_NAME=data        # Bucket MinIO para datos
```

**Dependencias principales:**
- `sodapy` - Cliente Socrata API
- `geopandas` - Cálculos geoespaciales
- `shapely` - Geometrías
- `pandas` - Manipulación de datos

### Ejecución

**Trigger manual:**
1. Abrir Airflow UI: `make airflow`
2. Localizar DAG: `etl_with_taskflow`
3. Click en "Play" para ejecutar

**Ejecución automática:**
- Schedule: `@monthly` (primer día de cada mes)
- Primera ejecución: descarga año completo
- Subsecuentes: solo último mes

### Monitoreo

**Ver logs:**
```bash
make logs                    # Todos los servicios
docker logs -f <container>   # Servicio específico
```

**Ver estado:**
```bash
make status
```

**Acceder a UIs:**
```bash
make airflow   # Ver DAG runs y logs
make minio     # Ver archivos en buckets
```

### Datos de Salida

**Ubicación:** MinIO bucket `data/`

**Estructura:**
```
data/
├── 0-raw-data/
│   ├── monthly-data/
│   │   └── {YYYY-MM}/
│   │       ├── crimes.csv              # Crímenes descargados del mes
│   │       └── police_stations.csv     # Estaciones policiales
│   └── data/
│       └── crimes_12m_{YYYY-MM-DD}.csv # Ventana rolling 12 meses
├── 1-enriched-data/
│   └── crimes_enriched_{YYYY-MM-DD}.csv # Crímenes enriquecidos
└── 2-processed-data/                    # (Próximos pasos)
    ├── train_encoded.csv
    ├── test_encoded.csv
    └── ...
```

## Apagar los servicios

Estos servicios ocupan cierta cantidad de memoria RAM y procesamiento, por lo que cuando no se están utilizando, se recomienda detenerlos. Para hacerlo, ejecuta:

```bash
make down
```

O usando docker-compose directamente:

```bash
docker compose --profile all down
```

Si deseas no solo detenerlos, sino también eliminar toda la infraestructura (liberando espacio en disco):

```bash
make clean
```

O usando docker-compose directamente:

```bash
docker compose down --rmi all --volumes
```

**Nota:** Si haces esto, perderás todo en los buckets y bases de datos.

## Aspectos específicos de Airflow

### Variables de entorno
Airflow ofrece una amplia gama de opciones de configuración. En el archivo `docker-compose.yaml`, dentro de `x-airflow-common`, se encuentran variables de entorno que pueden modificarse para ajustar la configuración de Airflow. Pueden añadirse [otras variables](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html).

### Uso de ejecutores externos
Actualmente, para este caso, Airflow utiliza un ejecutor [celery](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html), lo que significa que las tareas se ejecutan en otro contenedor. 

### Uso de la CLI de Airflow

Si necesitan depurar Apache Airflow, pueden utilizar la CLI de Apache Airflow de la siguiente manera:

```bash
docker compose --profile all --profile debug up
```

Una vez que el contenedor esté en funcionamiento, pueden utilizar la CLI de Airflow de la siguiente manera, 
por ejemplo, para ver la configuración:

```bash
docker-compose run airflow-cli config list      
```

Para obtener más información sobre el comando, pueden consultar [aqui](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html).

### Variables y Conexiones

Si desean agregar variables para accederlas en los DAGs, pueden hacerlo en `secrets/variables.yaml`. Para obtener más [información](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html), 
consulten la documentación.

Si desean agregar conexiones en Airflow, pueden hacerlo en `secrets/connections.yaml`. También es posible agregarlas mediante la interfaz de usuario (UI), pero estas no persistirán si se borra todo. Por otro lado, cualquier conexión guardada en `secrets/connections.yaml` no aparecerá en la UI, aunque eso no significa que no exista. Consulten la documentación para obtener más 
[información](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html).

## Conexión con los buckets

Dado que no estamos utilizando Amazon S3, sino una implementación local de los mismos mediante MinIO, es necesario modificar las variables de entorno para conectar con el servicio de MinIO. Las variables de entorno son las siguientes:

```bash
AWS_ACCESS_KEY_ID=minio   
AWS_SECRET_ACCESS_KEY=minio123 
AWS_ENDPOINT_URL_S3=http://localhost:90000
```

MLflow también tiene una variable de entorno que afecta su conexión a los buckets:

```bash
MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
```
Asegúrate de establecer estas variables de entorno antes de ejecutar tu notebook o scripts en tu máquina o en cualquier otro lugar. Si estás utilizando un servidor externo a tu computadora de trabajo, reemplaza localhost por su dirección IP.

Al hacer esto, podrás utilizar `boto3`, `awswrangler`, etc., en Python con estos buckets, o `awscli` en la consola.

Si tienes acceso a AWS S3, ten mucho cuidado de no reemplazar tus credenciales de AWS. Si usas las variables de entorno, no tendrás problemas.

## Valkey

La base de datos Valkey es usada por Apache Airflow para su funcionamiento. Tal como está configurado ahora no esta expuesto el puerto para poder ser usado externamente. Se puede modificar el archivo `docker-compose.yaml` para habilitaro.

## Pull Request

Este repositorio está abierto para que realicen sus propios Pull Requests y así contribuir a mejorarlo. Si desean realizar alguna modificación, **¡son bienvenidos!** También se pueden crear nuevos entornos productivos para aumentar la variedad de implementaciones, idealmente en diferentes `branches`. Algunas ideas que se me ocurren que podrían implementar son:

- Reemplazar Airflow y MLflow con [Metaflow](https://metaflow.org/) o [Kubeflow](https://www.kubeflow.org).
- Reemplazar MLflow con [Seldon-Core](https://github.com/SeldonIO/seldon-core).
- Agregar un servicio de tableros como, por ejemplo, [Grafana](https://grafana.com).

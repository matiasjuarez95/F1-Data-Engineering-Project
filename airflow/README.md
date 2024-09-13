# Extracción y Carga de Datos de Fórmula 1

## Descripción

Este proyecto está diseñado para extraer, procesar y cargar datos del dataset de Fórmula 1 que cubre desde 1950 hasta 2020. La primera fase del proyecto se centra en la extracción de datos desde Kaggle y su conversión a formato Parquet utilizando Docker y Apache Spark. La segunda fase integra Apache Airflow para la orquestación de tareas, que incluye la carga de los datos procesados en Google Cloud Storage (GCS).

## Estructura del Proyecto

F1-Data-Engineering-Project/ <br>
├── airflow/ <br>
│   ├── Dockerfile <br>
│   ├── docker-compose.yml <br>
│   ├── config/ <br>
│   ├── dags/ <br>
│   │   └── f1_to_gcs.py <br>
│   ├── logs/ <br>
│   ├── plugins/ <br>
│   ├── requirements.txt <br>
└── formula-1-output/ # Directorio para almacenar archivos Parquet

## Requisitos

- Docker
- Docker Compose
- Python 3.x
- Apache Spark
- Apache Airflow
- Google Cloud SDK (para autenticarte con GCS)

## Configuración del Entorno

1. **Configurar Kaggle**

   - Crea un archivo `kaggle.json` en la raíz del proyecto con tus credenciales de Kaggle. El archivo debe tener el siguiente formato:

     ```json
     {
       "username": "tu_usuario_kaggle",
       "key": "tu_clave_api_kaggle"
     }
     ```

2. **Configurar Google Cloud Storage**

   - Crea un archivo `gcp-cred.json` en la carpeta `airflow/` con las credenciales de tu cuenta de servicio de Google Cloud. Asegúrate de que el archivo tenga los permisos necesarios para escribir en el bucket de GCS.

3. **Preparar el Dockerfile para Airflow**

   - Asegúrate de tener el siguiente `Dockerfile` en la carpeta `airflow/` para construir la imagen de Docker para Airflow:

     ```Dockerfile
      FROM apache/airflow:2.10.1

      USER root

      RUN apt-get update && apt-get install -y \
          build-essential \
          openjdk-17-jdk \
          && apt-get clean

      ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
      ENV PATH=$JAVA_HOME/bin:$PATH

      USER airflow

      WORKDIR /opt/airflow
      COPY . /opt/airflow

      RUN pip install --no-cache-dir apache-airflow==2.10.1 -r requirements.txt
     ```

4. **Crear el archivo `requirements.txt` para Airflow**

   - El archivo `requirements.txt` debe contener las siguientes dependencias:

     ```
     pandas==2.1.2
     pyspark==3.5.0
     kaggle==1.5.13
     google-cloud-storage==2.10.0
     ```

5. **Escribir el Script de Extracción**

   - El archivo `extract-spark.py` contiene el código para descargar y procesar los datos. Este script realiza lo siguiente:
     - Descarga el dataset desde Kaggle.
     - Extrae los archivos del archivo ZIP.
     - Lee los archivos CSV y los convierte en DataFrames de Spark.
     - Aplica transformaciones y esquemas según sea necesario.
     - Guarda los DataFrames en formato Parquet en el directorio `formula-1-output`.

6. **Configurar el DAG en Airflow**

   - En la carpeta `airflow/dags/`, el archivo `f1_to_gcs.py` define el DAG para la extracción y carga de datos. Este DAG:
     - Ejecuta el script de extracción (`extract-spark.py`) usando un operador Bash.
     - Carga los archivos Parquet resultantes a Google Cloud Storage (GCS) utilizando el operador `GCS` de Airflow.

## Ejecución

1. **Construir la Imagen Docker para Airflow**

   - Ejecuta el siguiente comando en la carpeta `airflow/` para construir la imagen Docker:

     ```bash
     docker build -t airflow-f1-data-pipeline .
     ```

2. **Iniciar los Contenedores con Docker Compose**

   - Usa Docker Compose para iniciar los contenedores de Airflow y ejecutar el DAG:

     ```bash
     docker-compose up
     ```

   - Esto iniciará Airflow, permitirá la ejecución del DAG de extracción y carga, y almacenará los datos en el bucket de GCS configurado.

## Archivos Generados

- `formula-1-output/`: Contiene los archivos en formato Parquet generados a partir de los datos CSV procesados.
- Google Cloud Storage: Los archivos Parquet se cargarán en el bucket de GCS configurado en el DAG.

## Notas

- Asegúrate de que los archivos `kaggle.json` y `gcp-cred.json` estén correctamente configurados con tus credenciales antes de ejecutar el proceso.
- Los archivos Parquet generados estarán en el directorio `formula-1-output` y también se cargarán en GCS para su análisis posterior.
- Puedes monitorear la ejecución del DAG y verificar los resultados en la interfaz web de Airflow.


# Extracción de Datos de Fórmula 1

## Descripción

Este proyecto está diseñado para extraer y procesar datos del dataset de Fórmula 1 que cubre desde 1950 hasta 2020. La primera fase del proyecto se centra en la extracción de datos desde Kaggle y su conversión a formato Parquet utilizando Docker y Apache Spark.

## Estructura del Proyecto

F1-Data-Engineering-Project/ <br>
├── Dockerfile <br>
├── docker-compose.yml <br>
├── kaggle.json <br>
├── extract-spark.py <br>
├── requirements.txt <br>
└── formula-1-output/ # Directorio para almacenar archivos Parquet


## Requisitos

- Docker
- Python 3.x
- Apache Spark

## Configuración del Entorno

1. **Configurar Kaggle**

   - Crea un archivo `kaggle.json` en la raíz del proyecto con tus credenciales de Kaggle. El archivo debe tener el siguiente formato:

     ```json
     {
       "username": "tu_usuario_kaggle",
       "key": "tu_clave_api_kaggle"
     }
     ```

2. **Preparar el Dockerfile**

   - Asegúrate de tener el siguiente `Dockerfile` en la raíz del proyecto para construir la imagen de Docker:

     ```Dockerfile
     FROM python:latest

     WORKDIR /app
     COPY . /app

     RUN python3 -m pip install -r requirements.txt

     # Crear el directorio de salida
     RUN mkdir -p /app/formula-1-output
     ```

3. **Crear el archivo `requirements.txt`**

   - El archivo `requirements.txt` debe contener las siguientes dependencias:

     ```
     pandas==2.0.3
     pyspark==3.5.0
     kaggle==1.5.13
     ```

4. **Escribir el Script de Extracción**

   - Crea el archivo `extract-spark.py` que contiene el código para descargar y procesar los datos. Este script se encargará de:
     - Descargar el dataset desde Kaggle.
     - Extraer los archivos del archivo ZIP.
     - Leer los archivos CSV y convertirlos en DataFrames de Spark.
     - Aplicar transformaciones y esquemas según sea necesario.
     - Guardar los DataFrames en formato Parquet en el directorio `formula-1-output`.

## Ejecución

1. **Construir la Imagen Docker**

   - Ejecuta el siguiente comando en la raíz del proyecto para construir la imagen Docker:

     ```bash
     docker build -t f1-data-extraction .
     ```

2. **Ejecutar el Contenedor**

   - Usa Docker Compose para ejecutar el contenedor y ejecutar el script de extracción:

     ```bash
     docker-compose up
     ```

   - Esto descargará los datos de Kaggle, los procesará y los almacenará en el directorio `formula-1-output` en formato Parquet.

## Archivos Generados

- `formula-1-output/`: Contiene los archivos en formato Parquet generados a partir de los datos CSV procesados.

## Notas

- Asegúrate de que el archivo `kaggle.json` esté correctamente configurado con tus credenciales de Kaggle antes de ejecutar el proceso.
- Los archivos Parquet generados estarán en el directorio `formula-1-output`, y podrás utilizarlos para análisis posteriores en Apache Spark o cualquier otra herramienta que soporte Parquet.
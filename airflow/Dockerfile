# Usa una imagen base de Python
FROM apache/airflow:2.10.1

USER root

# Actualiza los repositorios e instala las herramientas necesarias
RUN apt-get update && apt-get install -y \
    build-essential \
    openjdk-17-jdk \
    && apt-get clean

# Configura variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Cambia al usuario airflow usando su UID
USER airflow

# Copia el script de carga y los archivos necesarios
WORKDIR /opt/airflow
COPY . /opt/airflow

# Instala las dependencias necesarias
RUN pip install --no-cache-dir apache-airflow==2.10.1 -r requirements.txt
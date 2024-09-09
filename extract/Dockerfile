# Usa una imagen base de Python
FROM python:3.12

# Actualiza los repositorios e instala Java
RUN apt-get update && apt-get install -y openjdk-17-jdk

# Configura variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copia el script de extracción y los archivos necesarios
WORKDIR /app
COPY . /app

# Instala las dependencias necesarias
RUN python3 -m pip install -r requirements.txt

# Comando por defecto para ejecutar el script
CMD ["python", "extract_spark.py"]
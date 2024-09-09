import pandas as pd
import os
import json
import zipfile
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Leer las credenciales del archivo kaggle.json
with open('./kaggle.json', 'r') as f:
    kaggle_creds = json.load(f)

# Establecer las variables de entorno
os.environ['KAGGLE_USERNAME'] = kaggle_creds['username']
os.environ['KAGGLE_KEY'] = kaggle_creds['key']

# Descargar el dataset
os.system('kaggle datasets download -d rohanrao/formula-1-world-championship-1950-2020')

# Ruta del archivo .zip
zip_file = './formula-1-world-championship-1950-2020.zip'
extract_folder = './Formula-1-Datasets'

# Abrir el archivo y extraer todo
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
  zip_ref.extractall(extract_folder)


# Directorio donde están los archivos CSV
csv_dir = './Formula-1-Datasets'
# Obtener la lista de archivos CSV en el directorio
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

spark = SparkSession.builder \
  .master("local[*]") \
  .appName('test') \
  .getOrCreate()

# Crear un diccionario para almacenar DataFrames de Spark
spark_dfs = {}

# Recorrer cada archivo CSV
for file in csv_files:
    # Extraer el nombre del archivo sin la extensión .csv
    df_name = file.replace('.csv', '')
    
    file_path = os.path.join(csv_dir, file)
    # Crear DataFrame de Spark
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    # Crear una variable dinámica para el DataFrame
    spark_dfs[df_name] = df



# Reemplazar '\N' por None en todos los DataFrames
for df_name, df in spark_dfs.items():
    for col_name in df.columns:
        df = df.withColumn(col_name, F.when(F.col(col_name) == r"\N", None).otherwise(F.col(col_name)))
    spark_dfs[df_name] = df

schemas = {
'circuits' : types.StructType([
    types.StructField('circuitId', types.IntegerType(), True), 
    types.StructField('circuitRef', types.StringType(), True), 
    types.StructField('name', types.StringType(), True), 
    types.StructField('location', types.StringType(), True), 
    types.StructField('country', types.StringType(), True), 
    types.StructField('lat', types.DoubleType(), True), 
    types.StructField('lng', types.DoubleType(), True), 
    types.StructField('alt', types.IntegerType(), True), 
    types.StructField('url', types.StringType(), True)
    ]),
'constructor_results' : types.StructType([
    types.StructField('constructorResultsId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('points', types.DoubleType(), True), 
    types.StructField('status', types.StringType(), True)
    ]),
'constructor_standings' : types.StructType([
    types.StructField('constructorStandingsId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('points', types.DoubleType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('positionText', types.StringType(), True), 
    types.StructField('wins', types.IntegerType(), True)
    ]),
'constructors' : types.StructType([
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('constructorRef', types.StringType(), True), 
    types.StructField('name', types.StringType(), True), 
    types.StructField('nationality', types.StringType(), True), 
    types.StructField('url', types.StringType(), True)
    ]),
'driver_standings' : types.StructType([
    types.StructField('driverStandingsId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('points', types.DoubleType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('positionText', types.StringType(), True), 
    types.StructField('wins', types.IntegerType(), True)
    ]),
'drivers' : types.StructType([
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('driverRef', types.StringType(), True), 
    types.StructField('number', types.StringType(), True), 
    types.StructField('code', types.StringType(), True), 
    types.StructField('forename', types.StringType(), True), 
    types.StructField('surname', types.StringType(), True), 
    types.StructField('dob', types.TimestampType(), True), 
    types.StructField('nationality', types.StringType(), True), 
    types.StructField('url', types.StringType(), True)
    ]),
'lap_times' : types.StructType([
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('lap', types.IntegerType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('time', types.TimestampType(), True), 
    types.StructField('milliseconds', types.IntegerType(), True)
    ]),
'pit_stops' : types.StructType([
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('stop', types.IntegerType(), True), 
    types.StructField('lap', types.IntegerType(), True), 
    types.StructField('time', types.TimestampType(), True), 
    types.StructField('duration', types.DoubleType(), True), 
    types.StructField('milliseconds', types.IntegerType(), True)
    ]),
'qualifying' : types.StructType([
    types.StructField('qualifyId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('number', types.IntegerType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('q1', types.StringType(), True), 
    types.StructField('q2', types.StringType(), True), 
    types.StructField('q3', types.StringType(), True)
    ]),
'races' : types.StructType([
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('year', types.IntegerType(), True), 
    types.StructField('round', types.IntegerType(), True), 
    types.StructField('circuitId', types.IntegerType(), True), 
    types.StructField('name', types.StringType(), True), 
    types.StructField('date', types.DateType(), True), 
    types.StructField('time', types.StringType(), True), 
    types.StructField('url', types.StringType(), True), 
    types.StructField('fp1_date', types.StringType(), True), 
    types.StructField('fp1_time', types.StringType(), True), 
    types.StructField('fp2_date', types.StringType(), True), 
    types.StructField('fp2_time', types.StringType(), True), 
    types.StructField('fp3_date', types.StringType(), True), 
    types.StructField('fp3_time', types.StringType(), True), 
    types.StructField('quali_date', types.StringType(), True), 
    types.StructField('quali_time', types.StringType(), True), 
    types.StructField('sprint_date', types.StringType(), True), 
    types.StructField('sprint_time', types.StringType(), True)
    ]),
'results' : types.StructType([
    types.StructField('resultId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('number', types.IntegerType(), True), 
    types.StructField('grid', types.IntegerType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('positionText', types.StringType(), True), 
    types.StructField('positionOrder', types.IntegerType(), True), 
    types.StructField('points', types.DoubleType(), True), 
    types.StructField('laps', types.IntegerType(), True), 
    types.StructField('time', types.StringType(), True), 
    types.StructField('milliseconds', types.IntegerType(), True), 
    types.StructField('fastestLap', types.IntegerType(), True), 
    types.StructField('rank', types.IntegerType(), True), 
    types.StructField('fastestLapTime', types.StringType(), True), 
    types.StructField('fastestLapSpeed', types.DoubleType(), True), 
    types.StructField('statusId', types.IntegerType(), True)
    ]),
'seasons' : types.StructType([
    types.StructField('year', types.IntegerType(), True), 
    types.StructField('url', types.StringType(), True)
    ]),
'sprint_results' : types.StructType([
    types.StructField('resultId', types.IntegerType(), True), 
    types.StructField('raceId', types.IntegerType(), True), 
    types.StructField('driverId', types.IntegerType(), True), 
    types.StructField('constructorId', types.IntegerType(), True), 
    types.StructField('number', types.IntegerType(), True), 
    types.StructField('grid', types.IntegerType(), True), 
    types.StructField('position', types.IntegerType(), True), 
    types.StructField('positionText', types.StringType(), True), 
    types.StructField('positionOrder', types.IntegerType(), True), 
    types.StructField('points', types.IntegerType(), True), 
    types.StructField('laps', types.IntegerType(), True), 
    types.StructField('time', types.StringType(), True), 
    types.StructField('milliseconds', types.IntegerType(), True), 
    types.StructField('fastestLap', types.StringType(), True), 
    types.StructField('fastestLapTime', types.StringType(), True), 
    types.StructField('statusId', types.IntegerType(), True)
    ]),
'status' : types.StructType([
    types.StructField('statusId', types.IntegerType(), True), 
    types.StructField('status', types.StringType(), True)
    ])
}

# Función para aplicar el esquema a un DataFrame
def apply_schema(df, schema):
    for field in schema:
        column_name = field.name
        column_type = field.dataType
        # Aplicar el cast si la columna existe en el DataFrame
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(column_type))
    return df

# Iterar sobre el diccionario de DataFrames y aplicar los esquemas
for df_name, df in spark_dfs.items():
    if df_name in schemas:
        schema = schemas[df_name]
        spark_dfs[df_name] = apply_schema(df, schema)


# Creamos los archivos parquet para luego cargar al Data Lake (google cloud storage)
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
for df_name, df in spark_dfs.items():
    output_path = f'/app/formula-1-output/{df_name}/'
    df.write.parquet(output_path)
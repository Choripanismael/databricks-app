# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta de raw a bronze
# MAGIC Lee el CSV de un dataset desde la capa raw en ADLS y lo carga en su tabla bronze.
# MAGIC El mismo notebook sirve para creditos y depositos, se elige con el parametro dataset.
# MAGIC El acceso al Data Lake es con Managed Identity por medio de la External Location ext_raw.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "adlsismael1004")
dbutils.widgets.dropdown("dataset", "creditos", ["creditos", "depositos"])

ambiente = dbutils.widgets.get("ambiente")
storage = dbutils.widgets.get("storage")
dataset = dbutils.widgets.get("dataset")

catalogo = f"cat_finanzas_{ambiente}"
ruta_raw = f"abfss://raw@{storage}.dfs.core.windows.net/bcrp/{dataset}/"
tabla_bronze = f"{catalogo}.bronze.{dataset}_departamento"

print(f"Leyendo {ruta_raw} hacia {tabla_bronze}")

# COMMAND ----------

# Esquema definido a mano para no depender de lo que infiera Spark
# En bronze todo se guarda como texto y los tipos se corrigen en silver
esquema = StructType([
    StructField("codigo_serie", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("moneda", StringType(), True),
    StructField("periodo", StringType(), True),
    StructField("valor", StringType(), True),
])

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .schema(esquema)
    .load(ruta_raw)
    .withColumn("archivo_origen", F.col("_metadata.file_path"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

total_filas = df.count()
if total_filas == 0:
    raise Exception(f"No se encontraron datos en {ruta_raw}")

print(f"Filas leidas {total_filas}")

# COMMAND ----------

# Carga completa porque la API del BCRP entrega toda la historia en cada descarga
columnas = ["codigo_serie", "departamento", "moneda", "periodo", "valor", "archivo_origen", "fecha_ingesta"]
df.select(columnas).write.mode("overwrite").insertInto(tabla_bronze)

print(f"Tabla {tabla_bronze} cargada con {spark.table(tabla_bronze).count()} filas")

# COMMAND ----------

display(spark.table(tabla_bronze).limit(10))

# Databricks notebook source
# MAGIC %md
# MAGIC ### Reversion de rutas fisicas
# MAGIC Borra las carpetas del ambiente en bronze silver y gold del Data Lake.
# MAGIC Se ejecuta despues de 01_drop_tablas.sql. La capa raw no se toca porque tiene los archivos originales.

# COMMAND ----------

dbutils.widgets.text("ambiente", "prod")
dbutils.widgets.text("storage", "adlsismael1004")

ambiente = dbutils.widgets.get("ambiente")
storage = dbutils.widgets.get("storage")

for contenedor in ["bronze", "silver", "gold"]:
    ruta = f"abfss://{contenedor}@{storage}.dfs.core.windows.net/{ambiente}"
    try:
        dbutils.fs.rm(ruta, recurse=True)
        print(f"Borrado {ruta}")
    except Exception as e:
        print(f"No se pudo borrar {ruta}")
        print(str(e)[:200])

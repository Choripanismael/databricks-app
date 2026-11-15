# Databricks notebook source
# MAGIC %md
# MAGIC ### Transformacion de bronze a silver
# MAGIC Limpia los saldos de creditos y depositos, convierte el periodo en fecha, pasa las monedas a columnas,
# MAGIC corrige los totales que no cuadran con la suma de moneda nacional y extranjera y une ambos datasets.
# MAGIC La carga a silver se hace con MERGE para poder reprocesar sin duplicar registros.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("ambiente", "dev")
ambiente = dbutils.widgets.get("ambiente")

catalogo = f"cat_finanzas_{ambiente}"
tabla_silver = f"{catalogo}.silver.saldos_departamento_mensual"

# Tolerancia para decidir si el total reportado no cuadra con MN mas ME
TOLERANCIA = 0.01

MESES = {
    "Ene": 1, "Feb": 2, "Mar": 3, "Abr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Ago": 8, "Sep": 9, "Set": 9, "Oct": 10, "Nov": 11, "Dic": 12,
}
mapa_meses = F.create_map([F.lit(x) for par in MESES.items() for x in par])

# COMMAND ----------


def limpiar(dataset):
    df = spark.table(f"{catalogo}.bronze.{dataset}_departamento")
    filas_bronze = df.count()
    if filas_bronze == 0:
        raise Exception(f"La tabla bronze de {dataset} esta vacia, primero ejecuta la ingesta de {dataset}")

    df = (
        df.dropDuplicates(["codigo_serie", "periodo"])
        .withColumn("departamento", F.trim(F.col("departamento")))
        .withColumn("moneda", F.upper(F.trim(F.col("moneda"))))
        .filter(F.col("departamento") != "Total")
        .withColumn("valor", F.col("valor").cast("double"))
        .withColumn("mes", mapa_meses[F.split(F.col("periodo"), r"\.").getItem(0)])
        .withColumn("anio", F.split(F.col("periodo"), r"\.").getItem(1).cast("int"))
        .withColumn("fecha", F.to_date(F.format_string("%d-%02d-01", F.col("anio"), F.col("mes"))))
    )

    descartadas = df.filter(F.col("valor").isNull() | F.col("fecha").isNull()).count()
    df = df.filter(F.col("valor").isNotNull() & F.col("fecha").isNotNull())
    print(f"{dataset} filas en bronze {filas_bronze} descartadas por valor o fecha invalida {descartadas}")

    # Una fila por departamento y mes con una columna por moneda
    df = (
        df.groupBy("departamento", "fecha", "anio", "mes")
        .pivot("moneda", ["MN", "ME", "TOTAL"])
        .agg(F.first("valor"))
    )

    # Regla de calidad, si el total no cuadra con MN mas ME se recalcula y se marca
    suma = F.col("MN") + F.col("ME")
    no_cuadra = F.abs(suma - F.col("TOTAL")) / F.col("TOTAL") > TOLERANCIA
    prefijo = "credito" if dataset == "creditos" else "deposito"

    df = (
        df.withColumn(f"flag_ajuste_{prefijo}", F.coalesce(no_cuadra, F.lit(False)))
        .withColumn("TOTAL", F.when(F.col(f"flag_ajuste_{prefijo}"), suma).otherwise(F.col("TOTAL")))
        .withColumnRenamed("MN", f"{prefijo}_mn")
        .withColumnRenamed("ME", f"{prefijo}_me")
        .withColumnRenamed("TOTAL", f"{prefijo}_total")
    )

    ajustados = df.filter(F.col(f"flag_ajuste_{prefijo}")).count()
    print(f"{dataset} registros con total recalculado {ajustados}")
    return df


creditos = limpiar("creditos")
depositos = limpiar("depositos")

# COMMAND ----------

silver = (
    creditos.join(depositos, on=["departamento", "fecha", "anio", "mes"], how="inner")
    .withColumn("fecha_proceso", F.current_timestamp())
    .select(
        "departamento", "fecha", "anio", "mes",
        "credito_mn", "credito_me", "credito_total",
        "deposito_mn", "deposito_me", "deposito_total",
        "flag_ajuste_credito", "flag_ajuste_deposito",
        "fecha_proceso",
    )
)

print(f"Registros para silver {silver.count()}")

# COMMAND ----------

# MERGE por departamento y fecha, actualiza lo que ya existe e inserta lo nuevo
destino = DeltaTable.forName(spark, tabla_silver)
(
    destino.alias("t")
    .merge(silver.alias("s"), "t.departamento = s.departamento AND t.fecha = s.fecha")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"Tabla {tabla_silver} con {spark.table(tabla_silver).count()} filas")

# COMMAND ----------

display(spark.table(tabla_silver).orderBy(F.col("fecha").desc(), "departamento").limit(30))

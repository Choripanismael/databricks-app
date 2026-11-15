# Databricks notebook source
# MAGIC %md
# MAGIC ### Carga de silver a gold
# MAGIC Calcula los indicadores que usa el dashboard.
# MAGIC Ratio credito sobre deposito, dolarizacion, variacion a 12 meses, participacion en el total nacional y ranking por mes.
# MAGIC Tambien arma un resumen anual con el saldo al cierre de cada anio.
# MAGIC Los saldos son de fin de mes, por eso el anual toma el ultimo mes y no suma los doce.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("ambiente", "dev")
ambiente = dbutils.widgets.get("ambiente")

catalogo = f"cat_finanzas_{ambiente}"
tabla_silver = f"{catalogo}.silver.saldos_departamento_mensual"
tabla_mensual = f"{catalogo}.gold.indicadores_departamento_mensual"
tabla_anual = f"{catalogo}.gold.resumen_anual_departamento"

silver = spark.table(tabla_silver)
if silver.count() == 0:
    raise Exception(f"La tabla {tabla_silver} esta vacia, primero ejecuta la transformacion")

# COMMAND ----------

# Ventanas que se usan en los calculos
por_departamento = Window.partitionBy("departamento").orderBy("fecha")
por_mes = Window.partitionBy("fecha")
ranking_mes_credito = Window.partitionBy("fecha").orderBy(F.col("credito_total").desc())
ranking_mes_deposito = Window.partitionBy("fecha").orderBy(F.col("deposito_total").desc())


def variacion(columna, ventana, desfase):
    anterior = F.lag(columna, desfase).over(ventana)
    return F.round((F.col(columna) / anterior - 1) * 100, 2)


mensual = (
    silver
    .withColumn("ratio_credito_deposito", F.round(F.col("credito_total") / F.col("deposito_total"), 4))
    .withColumn("dolarizacion_credito", F.round(F.col("credito_me") / F.col("credito_total") * 100, 2))
    .withColumn("dolarizacion_deposito", F.round(F.col("deposito_me") / F.col("deposito_total") * 100, 2))
    .withColumn("var_12m_credito", variacion("credito_total", por_departamento, 12))
    .withColumn("var_12m_deposito", variacion("deposito_total", por_departamento, 12))
    .withColumn("participacion_credito",
                F.round(F.col("credito_total") / F.sum("credito_total").over(por_mes) * 100, 2))
    .withColumn("participacion_deposito",
                F.round(F.col("deposito_total") / F.sum("deposito_total").over(por_mes) * 100, 2))
    .withColumn("ranking_credito", F.dense_rank().over(ranking_mes_credito))
    .withColumn("ranking_deposito", F.dense_rank().over(ranking_mes_deposito))
    .withColumn("credito_total", F.round("credito_total", 2))
    .withColumn("deposito_total", F.round("deposito_total", 2))
    .withColumn("fecha_proceso", F.current_timestamp())
    .select(
        "departamento", "fecha", "anio", "mes",
        "credito_total", "deposito_total", "ratio_credito_deposito",
        "dolarizacion_credito", "dolarizacion_deposito",
        "var_12m_credito", "var_12m_deposito",
        "participacion_credito", "participacion_deposito",
        "ranking_credito", "ranking_deposito",
        "fecha_proceso",
    )
)

mensual.write.mode("overwrite").insertInto(tabla_mensual)
print(f"Tabla {tabla_mensual} con {spark.table(tabla_mensual).count()} filas")

# COMMAND ----------

# Resumen anual con el saldo del ultimo mes disponible de cada anio
ultimo_mes = Window.partitionBy("departamento", "anio").orderBy(F.col("mes").desc())
por_anio = Window.partitionBy("departamento").orderBy("anio")
ranking_anio = Window.partitionBy("anio").orderBy(F.col("credito_cierre").desc())

dolarizacion_promedio = (
    silver.groupBy("departamento", "anio")
    .agg(F.round(F.avg(F.col("credito_me") / F.col("credito_total") * 100), 2).alias("dolarizacion_credito_prom"))
)

anual = (
    silver
    .withColumn("orden", F.row_number().over(ultimo_mes))
    .filter(F.col("orden") == 1)
    .select(
        "departamento", "anio",
        F.round("credito_total", 2).alias("credito_cierre"),
        F.round("deposito_total", 2).alias("deposito_cierre"),
    )
    .join(dolarizacion_promedio, on=["departamento", "anio"], how="left")
    .withColumn("crecimiento_credito", variacion("credito_cierre", por_anio, 1))
    .withColumn("crecimiento_deposito", variacion("deposito_cierre", por_anio, 1))
    .withColumn("ranking_credito", F.dense_rank().over(ranking_anio))
    .withColumn("fecha_proceso", F.current_timestamp())
    .select(
        "departamento", "anio", "credito_cierre", "deposito_cierre",
        "crecimiento_credito", "crecimiento_deposito",
        "dolarizacion_credito_prom", "ranking_credito", "fecha_proceso",
    )
)

anual.write.mode("overwrite").insertInto(tabla_anual)
print(f"Tabla {tabla_anual} con {spark.table(tabla_anual).count()} filas")

# COMMAND ----------

display(spark.table(tabla_mensual).filter(F.col("fecha") == F.lit("2025-12-01")).orderBy("ranking_credito"))

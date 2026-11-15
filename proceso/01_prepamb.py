# Databricks notebook source
# MAGIC %md
# MAGIC ### Preparacion de ambiente
# MAGIC Crea el catalogo del ambiente, los esquemas bronze silver y gold y las tablas vacias de cada capa.
# MAGIC Se puede ejecutar varias veces sin borrar nada porque todo usa IF NOT EXISTS.

# COMMAND ----------

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("storage", "adlsismael1004")

ambiente = dbutils.widgets.get("ambiente")
storage = dbutils.widgets.get("storage")

catalogo = f"cat_finanzas_{ambiente}"


def ruta(contenedor, carpeta):
    return f"abfss://{contenedor}@{storage}.dfs.core.windows.net/{ambiente}/{carpeta}"


print(f"Ambiente {ambiente} catalogo {catalogo}")

# COMMAND ----------

# Catalogo y esquemas
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalogo}
MANAGED LOCATION 'abfss://managed@{storage}.dfs.core.windows.net/{ambiente}'
COMMENT 'Observatorio de creditos y depositos por departamento ambiente {ambiente}'
""")

for esquema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{esquema} COMMENT 'Capa {esquema}'")

# COMMAND ----------

# Tablas bronze con los datos tal como llegan de raw mas columnas de auditoria
for dataset in ["creditos", "depositos"]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalogo}.bronze.{dataset}_departamento (
        codigo_serie STRING,
        departamento STRING,
        moneda STRING,
        periodo STRING,
        valor STRING,
        archivo_origen STRING,
        fecha_ingesta TIMESTAMP
    )
    USING DELTA
    LOCATION '{ruta("bronze", dataset + "_departamento")}'
    COMMENT 'Saldos de {dataset} por departamento sin transformar, fuente BCRP y SBS'
    """)

# COMMAND ----------

# Tabla silver con un registro por departamento y mes
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.silver.saldos_departamento_mensual (
    departamento STRING,
    fecha DATE,
    anio INT,
    mes INT,
    credito_mn DOUBLE,
    credito_me DOUBLE,
    credito_total DOUBLE,
    deposito_mn DOUBLE,
    deposito_me DOUBLE,
    deposito_total DOUBLE,
    flag_ajuste_credito BOOLEAN,
    flag_ajuste_deposito BOOLEAN,
    fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{ruta("silver", "saldos_departamento_mensual")}'
COMMENT 'Saldos limpios en millones de soles, un registro por departamento y mes'
""")

# COMMAND ----------

# Tablas gold para el dashboard
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.gold.indicadores_departamento_mensual (
    departamento STRING,
    fecha DATE,
    anio INT,
    mes INT,
    credito_total DOUBLE,
    deposito_total DOUBLE,
    ratio_credito_deposito DOUBLE,
    dolarizacion_credito DOUBLE,
    dolarizacion_deposito DOUBLE,
    var_12m_credito DOUBLE,
    var_12m_deposito DOUBLE,
    participacion_credito DOUBLE,
    participacion_deposito DOUBLE,
    ranking_credito INT,
    ranking_deposito INT,
    fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{ruta("gold", "indicadores_departamento_mensual")}'
COMMENT 'Indicadores mensuales por departamento'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.gold.resumen_anual_departamento (
    departamento STRING,
    anio INT,
    credito_cierre DOUBLE,
    deposito_cierre DOUBLE,
    crecimiento_credito DOUBLE,
    crecimiento_deposito DOUBLE,
    dolarizacion_credito_prom DOUBLE,
    ranking_credito INT,
    fecha_proceso TIMESTAMP
)
USING DELTA
LOCATION '{ruta("gold", "resumen_anual_departamento")}'
COMMENT 'Saldos al cierre de cada anio y crecimiento anual por departamento'
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalogo}.bronze")
        .union(spark.sql(f"SHOW TABLES IN {catalogo}.silver"))
        .union(spark.sql(f"SHOW TABLES IN {catalogo}.gold")))

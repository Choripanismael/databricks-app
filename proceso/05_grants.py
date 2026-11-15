# Databricks notebook source
# MAGIC %md
# MAGIC ### Permisos sobre las tablas
# MAGIC Da acceso de lectura a la capa gold al grupo de analistas y acceso completo de ingenieria al grupo de ingenieros.
# MAGIC Si un grupo no existe en la cuenta el permiso se salta y se avisa, para que el workflow no se caiga.

# COMMAND ----------

dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("grupo_analistas", "analistas_finanzas")
dbutils.widgets.text("grupo_ingenieros", "ingenieros_datos")

ambiente = dbutils.widgets.get("ambiente")
grupo_analistas = dbutils.widgets.get("grupo_analistas")
grupo_ingenieros = dbutils.widgets.get("grupo_ingenieros")

catalogo = f"cat_finanzas_{ambiente}"

# COMMAND ----------

permisos = [
    # Analistas solo leen gold, que es lo que alimenta el dashboard
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo_analistas}`",
    f"GRANT USE SCHEMA ON SCHEMA {catalogo}.gold TO `{grupo_analistas}`",
    f"GRANT SELECT ON SCHEMA {catalogo}.gold TO `{grupo_analistas}`",
    # Ingenieros pueden leer y escribir en todas las capas
    f"GRANT USE CATALOG ON CATALOG {catalogo} TO `{grupo_ingenieros}`",
    f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {catalogo}.bronze TO `{grupo_ingenieros}`",
    f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {catalogo}.silver TO `{grupo_ingenieros}`",
    f"GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA {catalogo}.gold TO `{grupo_ingenieros}`",
]

errores = 0
for sentencia in permisos:
    try:
        spark.sql(sentencia)
        print(f"OK {sentencia}")
    except Exception as e:
        errores += 1
        print(f"No se pudo aplicar {sentencia}")
        print(str(e)[:200])

print(f"Permisos aplicados {len(permisos) - errores} de {len(permisos)}")

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalogo}.gold"))

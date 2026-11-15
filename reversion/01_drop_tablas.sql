-- Reversion del despliegue
-- Se usa solo si el pase a produccion falla y hay que dejar el ambiente como estaba
-- Quita las tablas logicas, los esquemas y el catalogo de produccion
-- Las tablas son externas, por eso los archivos siguen en el Data Lake y se borran con 02_borrar_rutas.py

DROP TABLE IF EXISTS cat_finanzas_prod.gold.resumen_anual_departamento;
DROP TABLE IF EXISTS cat_finanzas_prod.gold.indicadores_departamento_mensual;
DROP TABLE IF EXISTS cat_finanzas_prod.silver.saldos_departamento_mensual;
DROP TABLE IF EXISTS cat_finanzas_prod.bronze.depositos_departamento;
DROP TABLE IF EXISTS cat_finanzas_prod.bronze.creditos_departamento;

DROP SCHEMA IF EXISTS cat_finanzas_prod.gold CASCADE;
DROP SCHEMA IF EXISTS cat_finanzas_prod.silver CASCADE;
DROP SCHEMA IF EXISTS cat_finanzas_prod.bronze CASCADE;

DROP CATALOG IF EXISTS cat_finanzas_prod CASCADE;

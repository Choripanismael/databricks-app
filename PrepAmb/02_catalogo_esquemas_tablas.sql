-- Preparacion de ambiente parte 2
-- Crea el catalogo, los esquemas y las tablas vacias de cada capa
-- Este script es para produccion, para desarrollo se cambia prod por dev en todo el archivo
-- El notebook proceso/01_prepamb hace lo mismo con el ambiente como parametro y es el que corre en el workflow

CREATE CATALOG IF NOT EXISTS cat_finanzas_prod
MANAGED LOCATION 'abfss://managed@adlsismael1004.dfs.core.windows.net/prod'
COMMENT 'Observatorio de creditos y depositos por departamento ambiente prod';

CREATE SCHEMA IF NOT EXISTS cat_finanzas_prod.bronze COMMENT 'Capa bronze';
CREATE SCHEMA IF NOT EXISTS cat_finanzas_prod.silver COMMENT 'Capa silver';
CREATE SCHEMA IF NOT EXISTS cat_finanzas_prod.gold COMMENT 'Capa gold';

CREATE TABLE IF NOT EXISTS cat_finanzas_prod.bronze.creditos_departamento (
    codigo_serie STRING,
    departamento STRING,
    moneda STRING,
    periodo STRING,
    valor STRING,
    archivo_origen STRING,
    fecha_ingesta TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@adlsismael1004.dfs.core.windows.net/prod/creditos_departamento'
COMMENT 'Saldos de creditos por departamento sin transformar, fuente BCRP y SBS';

CREATE TABLE IF NOT EXISTS cat_finanzas_prod.bronze.depositos_departamento (
    codigo_serie STRING,
    departamento STRING,
    moneda STRING,
    periodo STRING,
    valor STRING,
    archivo_origen STRING,
    fecha_ingesta TIMESTAMP
)
USING DELTA
LOCATION 'abfss://bronze@adlsismael1004.dfs.core.windows.net/prod/depositos_departamento'
COMMENT 'Saldos de depositos por departamento sin transformar, fuente BCRP y SBS';

CREATE TABLE IF NOT EXISTS cat_finanzas_prod.silver.saldos_departamento_mensual (
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
LOCATION 'abfss://silver@adlsismael1004.dfs.core.windows.net/prod/saldos_departamento_mensual'
COMMENT 'Saldos limpios en millones de soles, un registro por departamento y mes';

CREATE TABLE IF NOT EXISTS cat_finanzas_prod.gold.indicadores_departamento_mensual (
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
LOCATION 'abfss://gold@adlsismael1004.dfs.core.windows.net/prod/indicadores_departamento_mensual'
COMMENT 'Indicadores mensuales por departamento';

CREATE TABLE IF NOT EXISTS cat_finanzas_prod.gold.resumen_anual_departamento (
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
LOCATION 'abfss://gold@adlsismael1004.dfs.core.windows.net/prod/resumen_anual_departamento'
COMMENT 'Saldos al cierre de cada anio y crecimiento anual por departamento';

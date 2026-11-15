-- Permisos sobre las tablas del ETL en produccion
-- Los grupos analistas_finanzas e ingenieros_datos se crean en Settings, Identity and access, Groups
-- El notebook proceso/05_grants aplica lo mismo dentro del workflow

-- Analistas solo leen la capa gold que alimenta el dashboard
GRANT USE CATALOG ON CATALOG cat_finanzas_prod TO `analistas_finanzas`;
GRANT USE SCHEMA ON SCHEMA cat_finanzas_prod.gold TO `analistas_finanzas`;
GRANT SELECT ON SCHEMA cat_finanzas_prod.gold TO `analistas_finanzas`;

-- Ingenieros leen y escriben en todas las capas
GRANT USE CATALOG ON CATALOG cat_finanzas_prod TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA cat_finanzas_prod.bronze TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA cat_finanzas_prod.silver TO `ingenieros_datos`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA cat_finanzas_prod.gold TO `ingenieros_datos`;

-- Lectura de los archivos de la capa raw para los ingenieros
GRANT READ FILES ON EXTERNAL LOCATION ext_raw TO `ingenieros_datos`;

-- Para revisar los permisos aplicados
SHOW GRANTS ON SCHEMA cat_finanzas_prod.gold;

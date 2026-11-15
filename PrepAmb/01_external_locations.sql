-- Preparacion de ambiente parte 1
-- Registra los contenedores del Data Lake adlsismael1004 en Unity Catalog
-- El acceso es con Managed Identity por medio del Access Connector ac-finanzas-pe
-- La credencial cred_finanzas_pe se crea antes desde Catalog en Credentials con el Resource ID del conector

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_raw
URL 'abfss://raw@adlsismael1004.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_finanzas_pe)
COMMENT 'Capa raw con archivos originales';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze
URL 'abfss://bronze@adlsismael1004.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_finanzas_pe)
COMMENT 'Capa bronze';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_silver
URL 'abfss://silver@adlsismael1004.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_finanzas_pe)
COMMENT 'Capa silver';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_gold
URL 'abfss://gold@adlsismael1004.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_finanzas_pe)
COMMENT 'Capa gold';

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_managed
URL 'abfss://managed@adlsismael1004.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL cred_finanzas_pe)
COMMENT 'Almacenamiento de los catalogos';

#!/bin/bash
# Reversion del workflow en produccion
# Elimina el job WF_FINANZAS_PE usando la API de Databricks
# Uso
# export DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
# export DATABRICKS_TOKEN=token_de_produccion
# bash 03_eliminar_workflow.sh

WORKFLOW_NAME="WF_FINANZAS_PE"

job_id=$(curl -s -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "$DATABRICKS_HOST/api/2.1/jobs/list?name=$WORKFLOW_NAME" | jq -r '.jobs[]?.job_id')

if [ -z "$job_id" ]; then
  echo "No existe el workflow $WORKFLOW_NAME"
  exit 0
fi

for id in $job_id; do
  curl -s -X POST -H "Authorization: Bearer $DATABRICKS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"job_id\": $id}" \
    "$DATABRICKS_HOST/api/2.1/jobs/delete"
  echo "Workflow $id eliminado"
done

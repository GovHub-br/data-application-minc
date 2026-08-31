#!/usr/bin/env bash
#
# Gera dbt/minc/manifest.json, que o minc_cosmos_dag le no parse.
#
# Por que existe: por padrao o Cosmos monta a DAG rodando `dbt ls` durante o
# parse. Com o projeto atual isso leva ~35s e estoura o timeout do dag
# processor, e a DAG some da UI. Lendo um manifest pronto o parse volta a ser
# instantaneo.
#
# O manifest e um artefato VERSIONADO: precisa ser regerado e commitado sempre
# que modelo, source ou teste mudar, senao a DAG no Airflow reflete um projeto
# dbt que nao existe mais. O teste tests/test_dbt_manifest.py compara os dois.
#
# `dbt parse` nao conecta no banco -- da para rodar sem credencial do DW.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
projeto="${repo_root}/dbt/minc"
destino="${projeto}/manifest.json"
container="${AIRFLOW_CONTAINER:-data-application-minc-airflow-1}"

if docker ps --format '{{.Names}}' | grep -qx "${container}"; then
  echo "==> Gerando via container ${container}"
  docker exec -u airflow -w /opt/airflow/dbt/minc "${container}" \
    dbt parse --profiles-dir . --target-path /tmp/manifest_build --log-path /tmp/manifest_logs
  docker cp "${container}:/tmp/manifest_build/manifest.json" "${destino}"
else
  echo "==> Container ${container} nao esta no ar; usando o dbt local"
  (cd "${projeto}" && dbt parse --profiles-dir . --target-path /tmp/manifest_build --log-path /tmp/manifest_logs)
  cp /tmp/manifest_build/manifest.json "${destino}"
fi

modelos=$(python3 -c "
import json
d = json.load(open('${destino}'))
print(sum(1 for n in d['nodes'] if n.startswith('model.')))
")
echo "==> ${destino#"${repo_root}/"} escrito com ${modelos} modelos"
echo "==> Commite o manifest junto da mudanca no projeto dbt."

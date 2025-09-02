# GCP Data Pipeline — Energy & Environment

Pipeline serverless :  
Cloud Functions (ingestion) → GCS (raw) → BigQuery (ELT) → Workflows (orchestration) → Looker Studio.

## Objectif
Collecter des données de pollution/énergie, les stocker, transformer et visualiser.

## Structure
- function_ingest/ : Cloud Function
- bigquery/sql/ : scripts SQL BigQuery
- workflows/ : YAML Workflow GCP
- docs/ : schémas et captures

## Prochaines étapes
1. Créer le bucket GCS (energy-env-raw).
2. Créer le dataset BigQuery (energy_env).
3. Déployer la Cloud Function.
4. Exécuter les scripts SQL.
5. Déployer le Workflow.

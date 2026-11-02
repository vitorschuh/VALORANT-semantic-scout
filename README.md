# VALORANT Semantic Scout

Semantic scouting assistant over VALORANT match data: describe a round in natural language and get
similar rounds back. A data engineering practice project; the infrastructure is the hard part, not the volume.

**Scope:** VALORANT Champions 2026 (Shanghai), map Summit.

## Architecture
```
vlr.gg / valorant-api -> bronze (Iceberg) -contract-> silver -contract-> gold
  -> LLM round description -> embedding -> pgvector -> "similar rounds" search
```
- **Orchestration:** Airflow 3 (Astro CLI), one asset per table; downstream assets run when upstream ones update.
- **Lakehouse:** Iceberg tables as the source of truth, Parquet on MinIO, REST catalog.
- **Data contracts:** YAML contracts between layers; a failing contract blocks promotion.
- **Serving:** Postgres + pgvector (HNSW) holds the embeddings and is rebuildable from Iceberg.

## Layout
| Path | Purpose |
|---|---|
| `dags/` | Airflow assets (`valorant_static.py`, `vlr_matches.py`) |
| `include/lib/` | Iceberg catalog helper, contract runner, vlr.gg parser |
| `include/contracts/` | YAML data contracts |
| `include/sql/` | pgvector initialisation |
| `docker-compose.override.yml` | MinIO, Iceberg REST and pgvector next to the Astro stack |
| `tests/` | pytest suite (offline, vlr.gg pages saved as fixtures) |

## Run
```bash
cp .env.example .env
astro dev start      # also: astro dev stop | astro dev kill (kill wipes volumes)
```
- Airflow: http://localhost:8080 (admin/admin)
- MinIO console: http://localhost:9001 (minio/minio12345, bucket `warehouse`)
- Serving DB (pgvector): `localhost:5433`, user/password `valorant`, database `serving`

Trigger `bronze_agents` (static metadata) and `bronze_vlr_matches` (Champions 2026 matches);
the silver assets run when their bronze asset is updated.

## Tests
```bash
astro dev pytest
```

## Roadmap
1. Foundation and static metadata ingestion (done)
2. Match ingestion to bronze, bronze to silver with contracts (done); gold layer
3. Event-driven scheduling, backfill by patch
4. LLM round description, embeddings, load pgvector
5. Streamlit/FastAPI hybrid search (SQL filters + vector)
6. Embedding drift across patches, recall@k evaluation

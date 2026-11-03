# VALORANT Semantic Scout

A data and AI engineering practice project built on VALORANT esports data (vlr.gg). The match data is the
vehicle; the goal is to learn, hands-on, how a modern pipeline behaves when things go wrong.

## What this project is for
- **Airflow 3 in practice:** assets, event-driven scheduling (`AssetWatcher` on Kafka messages), human-in-the-loop
  operators, dynamic task mapping, pools for rate limiting, retries and backfill.
- **Data contracts at two levels:** *in transit* (messages arriving through Kafka) and *at rest* (datasets stored in the
  lake). A violation, such as a renamed column or a changed dtype, blocks promotion, quarantines the batch, produces a
  notification payload and waits for a human decision. Schema evolution is deliberately left for a later step.
- **Vectorisation in Postgres:** embeddings in pgvector (HNSW) and natural-language questions answered by routing
  between SQL and vector search.

It is not a match-analysis project: analytical depth is out of scope.

**Data scope:** currently VALORANT Champions 2026 (Shanghai), map Summit, kept small while the foundations are built.
The target is the whole VCT circuit from 2023 to 2026 (events, matches, rounds, players, teams, rosters, stats).

## Architecture
```
vlr.gg / valorant-api -> raw (S3) -> parse -> silver (Parquet, contract) -> gold (Postgres + pgvector)
                                        ^ Kafka carries parsed batches into Airflow (data contract in transit)
  gold -> embeddings -> natural-language questions (SQL + vector search)
```
- **Orchestration:** Airflow 3 (Astro CLI), one asset per dataset; downstream assets run when upstream ones update.
- **Data lake:** plain files on S3 (MinIO locally), laid out as `raw/`, `silver/`, `gold/` and `quarantine/`.
  Raw payloads are kept exactly as received; silver and gold are Parquet.
- **Data contracts:** YAML contracts checked in transit (Kafka messages) and at rest (stored datasets);
  a violation blocks promotion.
- **Serving:** Postgres + pgvector (HNSW) holds the embeddings and is rebuildable from the lake.

## Layout
| Path | Purpose |
|---|---|
| `dags/` | Airflow assets (`valorant_static.py`, `vlr_matches.py`) |
| `include/lib/` | Data lake helper, contract runner, vlr.gg parser |
| `include/contracts/` | YAML data contracts |
| `include/sql/` | pgvector initialisation |
| `docker-compose.override.yml` | MinIO, Kafka and pgvector next to the Astro stack |
| `tests/` | pytest suite (offline, vlr.gg pages saved as fixtures) |

## Run
```bash
cp .env.example .env
astro dev start      # also: astro dev stop | astro dev kill (kill wipes volumes)
```
- Airflow: http://localhost:8080 (admin/admin)
- MinIO console: http://localhost:9001 (minio/minio12345, bucket `lake`)
- Kafka: `localhost:9094` from the host, `kafka:9092` from the containers
- Serving DB (pgvector): `localhost:5433`, user/password `valorant`, database `serving`

Trigger `bronze_agents` (static metadata) and `bronze_vlr_matches` (Champions 2026 matches);
the silver assets run when their bronze asset is updated.

## Tests
```bash
astro dev pytest
```

## Roadmap
1. Foundation, static metadata and first match ingestion (done)
2. Stack reshape: S3 data lake instead of Iceberg, Kafka broker (done)
3. Contract engine: structured violations, quarantine, human-in-the-loop decision
4. Contract in transit: Kafka -> Airflow event-driven scheduling, dead-letter topic
5. Contract at rest: validation of stored datasets, cross-entity checks
6. Extraction at scale: VCT 2023-2026 (events, matches, rounds, players, rosters), mapped tasks, rate-limit pool, backfill
7. Gold and vectors: Postgres tables, local embeddings, pgvector load
8. Natural-language questions: routing between SQL and vector search

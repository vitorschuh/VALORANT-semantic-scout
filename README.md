# VALORANT Semantic Scout

Semantic scouting assistant over VALORANT match data (data engineering practice project).
See `CLAUDE.md` for architecture and roadmap.

## Run
```bash
cp .env.example .env
astro dev start
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

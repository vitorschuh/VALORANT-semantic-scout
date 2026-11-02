# VALORANT Semantic Scout

Semantic scouting assistant over Valorant match data. Data engineering practice project.

**Stack:** Astro CLI + Airflow 3 (assets, event-driven scheduling, airflow-ai-sdk operators),
Iceberg (REST catalog + MinIO), data contracts between layers, embeddings,
Postgres + pgvector (HNSW) as the serving layer. Iceberg is the source of truth.

**Flow:** APIs -> bronze (Iceberg) -contract-> silver -contract-> gold -> `@task.llm` round description
-> `@task.embed` -> pgvector -> "similar rounds" search.

**Scope:** start with one tournament (VALORANT Champions 2026 — Shanghai) and one map (Summit). Infra is the hard part, not volume.

## Layout
- `dags/` Airflow 3 assets (`from airflow.sdk import asset`)
- `include/lib/` Iceberg catalog helper, contract runner
- `include/contracts/` YAML data contracts (a failing contract blocks promotion)
- `include/sql/` serving DB init (pgvector)
- `docker-compose.override.yml` MinIO, Iceberg REST, pgvector next to the Astro stack
- `.env` config read by Airflow containers (local only)

## Commands
- `astro dev start` / `astro dev stop` / `astro dev kill` (kill wipes volumes)
- `astro dev pytest`
- UIs: Airflow http://localhost:8080, MinIO http://localhost:9001 (minio/minio12345), pgvector on localhost:5433

## Roadmap
1. Foundation + static metadata ingestion (done: `valorant_static.py`)
2. Match ingestion to bronze, bronze->silver with contracts (done: `vlr_matches.py`; gold still open, see handoff)
3. Event-driven scheduling, backfill by patch
4. LLM round classification/description, embeddings, load pgvector
5. Streamlit/FastAPI hybrid search (SQL filters + vector)
6. Embedding drift across patches, recall@k eval

## Session handoff (refinamento em andamento — remover esta seção quando aplicado)

Decisões tomadas (2026-11-01):
- **Fonte de dados de match/round**: vlr.gg (scraping/vlrggapi), round-level.
- **Campeonato alvo**: VALORANT Champions 2026 (Xangai) — base tanto do scraping quanto da geração de insights (substitui o "VCT Masters" genérico). Mapa único do escopo inicial: **Summit** (confirmar a grafia exata usada pelo vlr.gg antes de filtrar no scraping).
- **Provider LLM/embedding**: ainda não decidido (OpenAI vs Ollama local vs Claude+OpenAI) — decidir na fase 4.
- **Nome do projeto**: renomear pasta + repo para `VALORANT-semantic-scout` (produto final é busca semântica sobre rounds, não BI sobre matches).
- **Git**: ainda não inicializado. Depois do rename: `git init`, commit, criar repo no GitHub (público ou privado — perguntar ao usuário).

Fixes de fase 1: **aplicados em 2026-11-01** (contrato com type/allowed + `contract_path`, bronze cru com linhagem e flatten na silver,
`append_table` + partição + schema evolution explícito em `iceberg.py`, `round_embeddings` com `model`/`embedded_at`/PK `(round_id, model)`
e dimensão via `EMBEDDING_DIM`, connection `serving_db`, requirements pinados). `astro dev pytest`: 6 passed.

Smoke test `bronze_agents` → `silver_agents`: **passou em 2026-11-02** (30 agentes em bronze e silver, silver disparada pelo asset, `astro dev pytest`: 6 passed).
Problemas resolvidos no caminho:
- `db-migration` exit 1 era o `airflow-ai-sdk` 0.1.8 (registrado como provider, import circular no Airflow 3.3). Removido do `requirements.txt`; reavaliar na fase 4 (versão nova ou `pydantic-ai`/`openai` direto no lugar de `@task.llm`/`@task.embed`).
- Serviços do `docker-compose.override.yml` agora estão na rede `airflow` do Astro (senão `iceberg-rest`/`serving-db` não resolvem dentro do Airflow).
- Portas voltaram ao padrão (Airflow 8080, Postgres de metadados 5432); sem overrides em `.astro/config.yaml`.

Fase 2 (**implementada em 2026-11-02**): `dags/vlr_matches.py` + `include/lib/vlr.py` (parser puro, bs4) + contratos `silver_vlr_maps`/`silver_vlr_rounds`.
- Evento vlr.gg id 2766 (34 partidas, 86 mapas). `bronze.vlr_matches` guarda o HTML cru das 34 (append-only, só busca partidas ainda não ingeridas); o filtro Summit é na silver (18 mapas, 389 rounds).
- Grão da silver de rounds: vencedor, lados dos dois times, `win_type` (elim/boom/defuse/time), placar após o round, metade. Sem kill feed/economia por round (a aba de economia do vlr.gg não foi ingerida ainda).
- `played_at_raw` é string crua do vlr.gg (fuso não confirmado); converter antes do backfill por patch (fase 3).
- Bronze é `schedule=None` (manual). Silver dispara por asset. `astro dev pytest`: 11 passed.

Pendente:
1. Git: `git init`, commit, repo no GitHub (perguntar público/privado).
2. Decisão de produto antes da fase 4: com só esse grão, a descrição LLM de cada round é rasa. Opções: ingerir a aba de economia (`?game=<id>&tab=economy`) e/ou performance; montar o gold juntando round + composição + economia.
3. Gold (`gold.rounds_enriched`) e a fase 3 (agendamento por eventos, backfill por patch).

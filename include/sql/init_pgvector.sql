-- Run by init_pgvector.sh with: psql -v embedding_dim=<N>
-- (the dimension depends on the embedding model, still undecided — phase 4).
CREATE EXTENSION IF NOT EXISTS vector;

-- Serving layer. The data lake is the source of truth; these tables are rebuilt by the sync DAG.
-- One row per (round, model): lets us re-embed with a new model and compare (drift, recall@k).
CREATE TABLE IF NOT EXISTS round_embeddings (
    round_id    TEXT NOT NULL,
    match_id    TEXT NOT NULL,
    map         TEXT NOT NULL,
    patch       TEXT NOT NULL,
    round_type  TEXT,
    description TEXT,
    model       TEXT NOT NULL,
    embedded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    embedding   vector(:embedding_dim) NOT NULL,
    PRIMARY KEY (round_id, model)
);
CREATE INDEX IF NOT EXISTS round_embeddings_hnsw
    ON round_embeddings USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS round_embeddings_map_patch ON round_embeddings (map, patch);
CREATE INDEX IF NOT EXISTS round_embeddings_model_patch ON round_embeddings (model, patch);

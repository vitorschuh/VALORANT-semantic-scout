#!/bin/sh
# Postgres docker-entrypoint init hook (runs only on an empty data volume).
# Passes the embedding dimension into init_pgvector.sql as a psql variable.
set -e
psql -v ON_ERROR_STOP=1 -v embedding_dim="${EMBEDDING_DIM:-1536}" \
  --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -f /sql/init_pgvector.sql

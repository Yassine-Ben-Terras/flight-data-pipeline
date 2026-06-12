#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=reader_password="$GRAFANA_DB_PASSWORD" <<'SQL'
ALTER ROLE grafana_reader PASSWORD :'reader_password';
SQL

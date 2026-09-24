#!/usr/bin/env bash
# Create a full data dump of station_status.
#
#   scripts/dump.sh athena [STAGE]   one zstd parquet file per year in S3 (runs the
#                                    "dump station_status" Athena named query)
#   scripts/dump.sh duckdb [FILE]    a single local parquet file via DuckDB
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PROFILE=${AWS_PROFILE:-rd} AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}

case "${1:-}" in
athena)
  stage=${2:-prod}
  if [[ $stage == prod ]]; then wg=citibike db=citibike; else wg=citibike-$stage db=citibike_$stage; fi
  tag=$(date -u +%Y%m%d%H%M%S)
  qid=$(aws athena list-named-queries --work-group "$wg" --query 'NamedQueryIds' --output text | tr '\t' '\n' |
    while read -r id; do
      [[ $(aws athena get-named-query --named-query-id "$id" --query NamedQuery.Name --output text) == "dump station_status" ]] && echo "$id"
    done | head -1)
  sql=$(aws athena get-named-query --named-query-id "$qid" --query NamedQuery.QueryString --output text | sed "s/20260101/$tag/g")
  uv run scripts/athena.py --workgroup "$wg" --database "$db" "$sql" >/dev/null
  location=$(echo "$sql" | sed -n "s/.*external_location = '\(.*\)'.*/\1/p")
  aws s3 ls --recursive --human-readable "$location"
  echo "Dumped to $location (Athena table $db.station_status_dump_$tag)"
  ;;
duckdb)
  out=${2:-citibike_station_status.parquet}
  uv run python - "$out" <<'PY'
import sys, duckdb
con = duckdb.connect()
con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")
con.sql(f"""
COPY (
  FROM read_parquet('s3://insulator-citi-bikecaster/v2/station_status/*/*.parquet', hive_partitioning=true)
) TO '{sys.argv[1]}' (FORMAT parquet, COMPRESSION zstd)
""")
print(f"Wrote {sys.argv[1]}")
PY
  ;;
*)
  sed -n '2,7p' "$0"; exit 1 ;;
esac

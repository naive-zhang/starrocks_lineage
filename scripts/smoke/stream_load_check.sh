#!/usr/bin/env bash
# =====================================================================================================================
# Smoke test step 3.5: Stream Load exactly as StarRocksSink does it (FE redirect, Expect: 100-continue, JSON array,
# columns mapping with to_bitmap / hll_hash), then the same label again (must be "Label Already Exists").
# Usage: SR_FE_HOST=fe-host SR_USER=lineage_etl SR_PASSWORD=*** ./stream_load_check.sh
# =====================================================================================================================
set -euo pipefail
: "${SR_FE_HOST:?set SR_FE_HOST}" "${SR_USER:?set SR_USER}" "${SR_PASSWORD:?set SR_PASSWORD}"
HTTP_PORT="${SR_HTTP_PORT:-8030}"
TODAY="$(date +%F)"
LABEL="smoke_usage_$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Must stay identical to StarRocksSink.USAGE_COLUMNS
COLUMNS="stat_hour,catalog_name,db_name,table_name,column_name,access_query_cnt,select_clause_cnt,where_clause_cnt,join_clause_cnt,group_by_cnt,order_by_cnt,total_scan_bytes,total_query_time_ms,max_query_time_ms,last_access_time,user_id,user_name,user_bitmap=to_bitmap(user_id),user_hll=hll_hash(user_name)"

cat > "$WORK/usage.json" <<JSON
[
 {"stat_hour":"$TODAY 00:00:00","catalog_name":"__smoke__","db_name":"smoke","table_name":"t","column_name":"c",
  "access_query_cnt":1,"select_clause_cnt":1,"where_clause_cnt":0,"join_clause_cnt":0,"group_by_cnt":0,
  "order_by_cnt":0,"total_scan_bytes":100,"total_query_time_ms":10,"max_query_time_ms":10,
  "last_access_time":"$TODAY 00:10:00","user_id":1,"user_name":"__smoke_alice"},
 {"stat_hour":"$TODAY 00:00:00","catalog_name":"__smoke__","db_name":"smoke","table_name":"t","column_name":"c",
  "access_query_cnt":1,"select_clause_cnt":0,"where_clause_cnt":1,"join_clause_cnt":0,"group_by_cnt":0,
  "order_by_cnt":0,"total_scan_bytes":200,"total_query_time_ms":30,"max_query_time_ms":30,
  "last_access_time":"$TODAY 00:20:00","user_id":2,"user_name":"__smoke_bob"}
]
JSON

cat > "$WORK/dict.json" <<JSON
[{"user_name":"__smoke_alice","user_id":1,"first_seen_time":"$TODAY 00:10:00"},
 {"user_name":"__smoke_bob","user_id":2,"first_seen_time":"$TODAY 00:20:00"}]
JSON

load() { # table label columns file
  curl --silent --show-error --location-trusted -u "$SR_USER:$SR_PASSWORD" \
       -H "Expect: 100-continue" -H "label: $2" -H "format: json" -H "strip_outer_array: true" \
       -H "columns: $3" -T "$4" "http://$SR_FE_HOST:$HTTP_PORT/api/audit_lineage_db/$1/_stream_load"
  echo
}

echo "== 1. usage load (expected Status=Success, NumberLoadedRows=2)"
load audit_column_hourly_usage "$LABEL" "$COLUMNS" "$WORK/usage.json"
echo "== 2. same label again (expected Status=Label Already Exists, ExistingJobStatus=FINISHED)"
load audit_column_hourly_usage "$LABEL" "$COLUMNS" "$WORK/usage.json"
echo "== 3. user dictionary load (expected Status=Success, NumberLoadedRows=2)"
load audit_user_dict "${LABEL}_dict" "user_name,user_id,first_seen_time" "$WORK/dict.json"
echo "== now run section 3.5 of 03_manual_checks.sql"

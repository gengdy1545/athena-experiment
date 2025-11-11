#!/usr/bin/env bash
# TPC-H generator: stream each .tbl directly to S3 with dbgen progress.
# Usage:
#   ./tpch_to_s3.sh <SCALE_FACTOR> <S3_PREFIX>
# Example:
#   ./tpch_to_s3.sh 10 s3://my-bucket/tpch/sf-10/
#
# Requirements:
#   - gcc, make, git
#   - AWS CLI v2 configured (aws s3 cp)
#   - network access to clone electrum/tpch-dbgen
#
# Notes:
#   - No local .tbl files are created; each table is streamed via a FIFO.
#   - dbgen progress is shown with -v (verbose).
#   - Tables are generated one-by-one using -T flags (see mapping below).

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <SCALE_FACTOR> <S3_PREFIX>"
  exit 1
fi

SF="$1"
S3_PREFIX="$2"
S3_PREFIX="${S3_PREFIX%/}/"    # ensure trailing slash

# Clone & build dbgen if needed
if [[ ! -d tpch-dbgen ]]; then
  echo "--- Cloning tpch-dbgen ---"
  git clone https://github.com/electrum/tpch-dbgen.git
fi

echo "--- Building dbgen ---"
pushd tpch-dbgen >/dev/null
make
popd >/dev/null

# Temporary working dir for FIFOs; no .tbl persisted
WORKDIR="$(mktemp -d)"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

DBGEN="./tpch-dbgen/dbgen"

# Helper: stream a single table directly to S3 using a FIFO
# Args: <table_letter> <table_filename>
stream_table() {
  local TLETTER="$1"
  local FNAME="$2"             # e.g., customer.tbl
  local FIFO_PATH="$WORKDIR/$FNAME"
  local S3_URI="${S3_PREFIX}${FNAME}"

  echo "--- Generating ${FNAME} (SF=${SF}) -> ${S3_URI} ---"

  # Create FIFO that dbgen will write into (via DSS_PATH).
  mkfifo "$FIFO_PATH"

  # Uploader consumes from the FIFO and writes to S3 (stdin is "-").
  # Use a subshell to ensure proper job control and 'set -e' behavior.
  ( set -euo pipefail; cat "$FIFO_PATH" | aws s3 cp - "$S3_URI" ) &
  UP_PID=$!

  # Point dbgen to write into WORKDIR and generate only this table.
  # -s <SF>   : scale factor
  # -T <char> : select single table
  # -v        : show progress (dbgen prints progress to stderr)
  # -f        : force without prompts
  DSS_PATH="$WORKDIR" DSS_CONFIG="tpch-dbgen" \
    "$DBGEN" -s "$SF" -T "$TLETTER" -v -f 2>&1

  # Wait for upload to finish, then remove FIFO
  wait "$UP_PID"
  rm -f "$FIFO_PATH"
  echo "--- Uploaded ${FNAME} ---"
}

# Table letter mapping for TPC-H dbgen (-T):
#   c: customer      n: nation        r: region
#   s: supplier      P: part          S: partsupp
#   O: orders        L: lineitem
# (There are combo flags like 'o' and 'p', but we use single-table flags.) :contentReference[oaicite:1]{index=1}

# Recommended order (dims before facts):
stream_table n nation.tbl
stream_table r region.tbl
stream_table c customer.tbl
stream_table s supplier.tbl
stream_table P part.tbl
stream_table S partsupp.tbl
stream_table O orders.tbl
stream_table L lineitem.tbl

echo "--- Done: TPC-H SF=${SF} streamed to ${S3_PREFIX} ---"

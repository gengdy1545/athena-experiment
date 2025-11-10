#!/bin/bash

# *****************************************************
# Optimized TPC-H Raw Data Generation Script
#
# This script is used on an EC2 instance to:
# 1. Check for dependencies (aws-cli, pv).
# 2. Compile the TPC-H data generator (dbgen).
# 3. Generate raw .tbl (pipe-delimited) files for multiple scale factors.
# 4. Stream the output of dbgen directly to an S3 staging bucket
#    in parallel (8 tables at a time per scale factor) to save disk space
#    and improve speed.
# 5. Display progress for each table upload using 'pv'.
#
# Usage:
# ./generate_raw_data.sh <s3_staging_bucket_path>
# Example:
# ./generate_raw_data.sh s3://my-bucket/staging-tpch-raw
#
# Prerequisites:
# - git, make, gcc (for compiling dbgen)
# - aws-cli (for S3 upload)
# - pv (for progress monitoring)
#   (Install on Ubuntu/Debian: sudo apt update && sudo apt install -y pv)
#   (Install on RHEL/CentOS/Amazon Linux: sudo yum install -y pv)
# *****************************************************

set -e
echo "--- Starting TPC-H raw data generation ---"

# --- 1. Validate Input Argument & Dependencies ---

# Check if the required argument (S3 path) is provided
if [ -z "$1" ]; then
    echo "Error: Missing required argument."
    echo "Usage: $0 <s3_staging_bucket_path>"
    echo "Example: $0 s3://my-staging-bucket/staging-tpch-raw"
    exit 1
fi

# The first argument is the S3 staging bucket path
STAGING_BUCKET=$1

# Ensure S3 path has a trailing slash for correct path joining
if [[ "${STAGING_BUCKET}" != */ ]]; then
    STAGING_BUCKET="${STAGING_BUCKET}/"
fi

# Check for required command-line tools
command -v aws >/dev/null 2>&1 || { echo >&2 "Error: 'aws' CLI not found. Please install and configure it."; exit 1; }
command -v pv >/dev/null 2>&1 || { echo >&2 "Error: 'pv' (Pipe Viewer) not found. Please install it (e.g., sudo apt install pv)."; exit 1; }
command -v git >/dev/null 2>&1 || { echo >&2 "Error: 'git' not found. Please install it."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "Error: 'make' not found. Please install build-essential or equivalent."; exit 1; }

# --- 2. Build TPC-H dbgen ---
echo "--- Cloning and building TPC-H dbgen ---"

# Clone only if the directory doesn't exist
if [ ! -d "tpch-dbgen" ]; then
    # You can change this URL if you have a different fork of dbgen
    git clone https://github.com/electrum/tpch-dbgen.git
    cd tpch-dbgen
    # Note: TPC-H defaults to the C90 standard.
    # If 'make' fails, you may need to modify the Makefile:
    # CFLAGS = $(CDEF) -Wno-error=implicit-function-declaration
    make
    cd ..
else
    echo "tpch-dbgen directory already exists, skipping clone and build."
fi

cd tpch-dbgen

# --- 3. Define Functions ---

# This function generates a *single* table and streams it to S3.
# It uses the -T flag for dbgen to specify which table to build.
# Arguments:
#   $1: Scale Factor (e.g., 10)
#   $2: S3 folder name (e.g., sf-10)
#   $3: Table character (dbgen flag, e.g., 'c' for customer)
#   $4: Table name (e.g., 'customer')
generate_table_and_upload() {
    local SF=$1
    local S3_FOLDER=$2
    local TBL_CHAR=$3
    local TBL_NAME=$4
    local TARGET_S3_PATH="${STAGING_BUCKET}${S3_FOLDER}/${TBL_NAME}.tbl"

    echo "[SF-${SF}] Generating table: ${TBL_NAME} -> ${TARGET_S3_PATH}"

    # -s: Scale Factor
    # -T: Generate a specific table
    #     c: customer
    #     L: lineitem (Note: Some dbgen versions use 'L', others 'l'. Check with './dbgen -h')
    #     n: nation
    #     o: orders
    #     p: part
    #     S: partsupp (Note: Uppercase 'S')
    #     r: region
    #     s: supplier (Note: Lowercase 's')
    # -v: Verbose mode (sends progress to stderr, data to stdout)

    # We pipe the output (stdout) of dbgen directly to pv (for progress)
    # and then pipe that to 'aws s3 cp' using '-' to read from stdin.
    ./dbgen -s ${SF} -T ${TBL_CHAR} -v 2>&1 | \
        grep -v 'CREATE TABLE' | \
        pv -N "[SF-${SF}] ${TBL_NAME}" | \
        aws s3 cp - "${TARGET_S3_PATH}"

    echo "[SF-${SF}] Completed upload for: ${TBL_NAME}"
}

# This function orchestrates the parallel generation for all 8 tables
# for a single scale factor.
# Argument 1 ($1): Scale Factor (e.g., 10)
process_scale_factor() {
    local SF=$1
    local S3_FOLDER="sf-${SF}"

    echo "=========================================================="
    echo "--- Starting parallel data generation for SF=${SF}... ---"
    echo "--- Target S3 location: ${STAGING_BUCKET}${S3_FOLDER}/ ---"
    echo "=========================================================="

    # Start all 8 table generation processes in the background
    generate_table_and_upload $SF $S3_FOLDER c customer &
    generate_table_and_upload $SF $S3_FOLDER o orders &
    generate_table_and_upload $SF $S3_FOLDER L lineitem & # Using 'L' for lineitem
    generate_table_and_upload $SF $S3_FOLDER p part &
    generate_table_and_upload $SF $S3_FOLDER S partsupp & # Using 'S' for partsupp
    generate_table_and_upload $SF $S3_FOLDER s supplier & # Using 's' for supplier
    generate_table_and_upload $SF $S3_FOLDER n nation &
    generate_table_and_upload $SF $S3_FOLDER r region &

    # 'wait' ensures the script pauses until all background jobs are finished
    echo "[SF-${SF}] Waiting for all 8 tables to complete generation and upload..."
    wait
    echo "[SF-${SF}] All tables for SF=${SF} have been uploaded."
}

# --- 4. Run for all scales ---
# Run for all required scales 10G (SF-10), 30G (SF-30), and 100G (SF-100)
# These will run sequentially (SF-10 must finish before SF-30 starts),
# but within each SF, the 8 tables are generated/uploaded in parallel.

process_scale_factor 10
process_scale_factor 30
process_scale_factor 100

# Go back to the original directory
cd ..

echo "--- All TPC-H raw data has been generated and uploaded to ${STAGING_BUCKET} ---"
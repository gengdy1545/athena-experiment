#!/bin/bash

# *****************************************************
# This script is used on an EC2 instance to compile the TPC-H data generator (dbgen),
# generate the raw .tbl (CSV) files, and upload them to an S3 staging bucket.
#
# Usage:
# ./generate_data.sh <s3_staging_bucket_path>
# Example:
# ./generate_data.sh s3://my-bucket/staging-tpch-raw
# *****************************************************

set -e
echo "--- Starting TPC-H raw data generation ---"

# --- 1. Validate Input Argument ---
# Check if the required argument (S3 path) is provided
if [ -z "$1" ]; then
    echo "Error: Missing required argument."
    echo "Usage: $0 <s3_staging_bucket_path>"
    echo "Example: $0 s3://my-staging-bucket/staging-tpch-raw"
    exit 1
fi

# The first argument is the S3 staging bucket path
STAGING_BUCKET=$1

# --- 2. Build TPC-H dbgen ---
echo "--- Cloning and building TPC-H dbgen ---"
# You can change this URL if you have a different fork of dbgen
git clone git@github.com:electrum/tpch-dbgen.git
cd tpch-dbgen
# Note: TPC-H defaults to the C90 standard, which may require adjustments for newer GCC versions
# If 'make' fails, try modifying the Makefile: CFLAGS = $(CDEF) -Wno-error=implicit-function-declaration
make

# --- 3. Define Function ---
# Define the generation and upload function
# Argument 1 ($1): Scale Factor (e.g., 10)
# Argument 2 ($2): S3 folder name (e.g., sf-10)
generate_and_upload() {
    local SF=$1
    local S3_FOLDER=$2
    local TARGET_S3_PATH="${STAGING_BUCKET}/${S3_FOLDER}/"

    echo "--- Generating data for SF=${SF}... ---"

    # Clean up old files (if they exist)
    rm -f *.tbl

    # Run dbgen
    # -s $SF specifies the scale factor (size)
    ./dbgen -s $SF

    echo "--- Data generation complete (SF=${SF}). Uploading to ${TARGET_S3_PATH} ---"

    # Use AWS CLI to sync all .tbl files to S3
    aws s3 sync . ${TARGET_S3_PATH} --exclude "*" --include "*.tbl"

    echo "--- SF=${SF} upload complete ---"
    rm -f *.tbl
}

# --- 4. Run for all scales ---
# Run for all required scales 10G (SF-10), 30G (SF-30), and 100G (SF-100)

generate_and_upload 10 "sf-10"

generate_and_upload 30 "sf-30"

generate_and_upload 100 "sf-100"

echo "--- All TPC-H raw data has been generated and uploaded to ${STAGING_BUCKET} ---"
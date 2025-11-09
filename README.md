# Athena Experiment
Connect to AWS Athena to submit queries with fixed load and collect results.

## Setup

### 1. Generate TPC-H Data
Run `scripts/generate_raw_data.sh` to create raw TPC-H data files and 
upload them to your specified S3 bucket.

### 2. Convert Data to Parquet

Click "ETL Jobs" and select "Spark Script editor".
Paste the contents of `scripts/glue_conversion_job.py` into the script editor.

* Type: `Spark`
* Number of workers: `30`

Run jobs with different parameters for different scale factors,
e.g., for SF-10:
* scale_factor=10
* target_s3_folder=tpch_0

## 3. Create Glue Database for Parquet Data
Create Glue database to catalog the Parquet data, including `tpch_db_0` to `tpch_db_4`.

Click "Crawlers" and then click "Add crawler" to create Glue crawlers for each database.

* Crawler details: e.g. `tpch_0_crawler`
* Data sources
  * Data source: `s3`
  * S3 path: e.g. `s3://{bucket}/staging-tpch-raw/tpch_0/`
  * Parameters: `Recrawl all`
* Target database: e.g. `tpch_db_0`
* Create a single schema for each S3 path: `UnChecked`

After run all crawlers, you can see `tpch_db_0` to `tpch_db_4` databases with corresponding 8 tables.

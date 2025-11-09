# Athena Experiment
Connect to AWS Athena to submit queries with fixed load and collect results.

## Setup

### Generate TPC-H Data
Run `scripts/generate_raw_data.sh` to create raw TPC-H data files and 
upload them to your specified S3 bucket.

### Configure AWS Glue
Use Glue to parse .tbl files in the S3 staging area.

#### 1. create a custom classifier
Click "Classifiers" and then click "Add classifier".

* Classifier name: `tpch_pipe_delimiter`
* Classifier type: `CSV`
* Column delimiter: `|`
* Quote symbol: `Double quote (")`
* Column headings: `No headings`
* Allow single column: `False`
* Disable value trimming: `True`

#### 2. create glue database
Click "Databases" and then click "Add database".

* Database name: `tpch_raw_staging`

#### 3. create glue crawler
Click "Crawlers" and then click "Add crawler".

* Crawler details: `tpch_staging_crawler`
* Data sources
  * Data source: `s3`
  * S3 path: e.g. `s3://{bucket}/staging-tpch-raw/sf-10/`
  * Parameters: `Recrawl all`
* Classifiers: `tpch_pipe_delimiter`
* Target database: `tpch_raw_staging`
* Table name prefix: e.g. `sf_10_`
* Create a single schema for each S3 path: `UnChecked`

Under the `tpch_raw_staging` database, tables prefixed with
`sf_10_` will be generated, such as `sf_10_customer_tbl`.

Other tables can be generated in the same manner.

### Convert Data to Parquet

Click "ETL Jobs" and select "Spark Script editor".
Paste the contents of `scripts/glue_conversion_job.py` into the script editor.

* Type: `Spark`
* Number of workers: `10` for SF-10/30, `30` for SF-100

Run jobs with different parameters for different scale factors,
e.g., for SF-10:
* source_sf_prefix=sf_10
* target_s3_folder=tpch_0
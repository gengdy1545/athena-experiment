# TPC-H Data Experiment on AWS Athena
This project provides a complete framework for setting up and running TPC-H benchmarks against Amazon Athena.
It includes:

1. **Data Setup:** Scripts to generate, convert, and catalog TPC-H data at various scale factors using S3, AWS Glue, and EC2.
2. **Benchmark Tool:** A Java-based tool to replay query workloads against the cataloged data, simulating real-world concurrency and capturing detailed performance and cost metrics.

## Part 1: TPC-H Data Setup Workflow

### 1. Generate TPC-H Raw Data
Ensure you have `git`, `make`, `gcc`, and the `aws-cli` installed.

Run `scripts/generate_raw_data.sh` script, passing your S3 staging bucket path as the first argument. 
The script will generate data for multiple scale factors (e.g., SF-10, SF-30, SF-100) and place them in corresponding subfolders.

```bash
# Usage: ./generate_data.sh <s3_staging_bucket_path>

# Example:
./scripts/generate_data.sh s3://your-bucket-name/staging-tpch-raw
```

After this step, your S3 bucket should contain:
* s3://your-bucket-name/staging-tpch-raw/sf-10/
* s3://your-bucket-name/staging-tpch-raw/sf-30/
* ...

### 2. Convert Data to Parquet (AWS Glue Job)
Convert the raw pipe-delimited .tbl files into Parquet format, which is highly optimized for Athena

1. Navigate to the **AWS Glue** service in your console.
2. Go to **ETL jobs** and choose **Spark script editor**.
3. Create a new job by pasting the entire contents of `scripts/glue_conversion_job.py` into the script editor.
4. Configure the **Job details**:
    - **Name:** e.g., `tpch_conversion`
    - **IAM Role:** Select a role with S3 read/write and Glue permissions.
5. Configure the **Job parameters:**
    - **Type:** `Spark`
    - **Glue version:** `5.0`
    - **Worker type:** `G 2x`
    - **Number of workers**: `30`
6. This script is parameterized. You must run the job multiple times—once for each scale factor you want to process.
In the Job parameters section, provide the following arguments:
    * `scale_factor`: e.g., `10`, `30`, `100`
    * `target_s3_folder`: e.g., `tpch_0`
    * `raw_data_base_path`: `s3://your-bucket-name/staging-tpch-raw/`
    * `athena_base_path`: `s3://your-bucket-name/athena-data/`
7. Run the job for each scale factor.

## 3. Create Glue Database & Crawler
Catalog the Parquet data, making it discoverable by Athena.

1. In the AWS Glue console, Navigate to **Crawlers** and click **Add crawler**.
2. Create a new crawler for each dataset you converted (e.g., one for `tpch_0`).
3. Crawler Configuration (Example for `tpch_0`):
    * **Crawler name:** `tpch_0_crawler`
    * **Data source:**
      * **S3 path:** Point to the output folder from Step 2 (e.g., s3://your-bucket-name/athena-data/tpch_0/)
      * **IAM role:** Choose a role with S3 read access and Glue catalog permissions.
      * **Target database:**
        * Click **Add database** to create a new one.
        * **Database name:** `tpch_db_0`
    * **Crawler schedule:** `Run on demand`
    * **Create a single schema for each S3 path:** `UnChecked` 
4. Run the crawler. When it completes, your `tpch_db_0` database will be populated with the 8 TPC-H tables.
5. Repeat this process to create crawlers and databases for your other datasets.

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, DateType

# --- Required: Get Job Parameters ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'scale_factor',       # e.g., "10", "30", "100"
    'target_s3_folder'    # e.g., "tpch_0" (The target folder you want to write to)
    'raw_data_base_path', # e.g., "s3://your-bucket/staging-tpch-raw/"
    'athena_base_path'    # e.g., "s3://your-bucket/athena/"
])

# --- 1. Define TPC-H Full Schemas ---
schemas = {
    "customer": StructType([
        StructField("c_custkey", LongType(), True),
        StructField("c_name", StringType(), True),
        StructField("c_address", StringType(), True),
        StructField("c_nationkey", LongType(), True),
        StructField("c_phone", StringType(), True),
        StructField("c_acctbal", DecimalType(12, 2), True),
        StructField("c_mktsegment", StringType(), True),
        StructField("c_comment", StringType(), True)
    ]),
    "lineitem": StructType([
        StructField("l_orderkey", LongType(), True),
        StructField("l_partkey", LongType(), True),
        StructField("l_suppkey", LongType(), True),
        StructField("l_linenumber", IntegerType(), True),
        StructField("l_quantity", DecimalType(12, 2), True),
        StructField("l_extendedprice", DecimalType(12, 2), True),
        StructField("l_discount", DecimalType(12, 2), True),
        StructField("l_tax", DecimalType(12, 2), True),
        StructField("l_returnflag", StringType(), True),
        StructField("l_linestatus", StringType(), True),
        StructField("l_shipdate", DateType(), True),
        StructField("l_commitdate", DateType(), True),
        StructField("l_receiptdate", DateType(), True),
        StructField("l_shipinstruct", StringType(), True),
        StructField("l_shipmode", StringType(), True),
        StructField("l_comment", StringType(), True)
    ]),
    "nation": StructType([
        StructField("n_nationkey", LongType(), True),
        StructField("n_name", StringType(), True),
        StructField("n_regionkey", LongType(), True),
        StructField("n_comment", StringType(), True)
    ]),
    "orders": StructType([
        StructField("o_orderkey", LongType(), True),
        StructField("o_custkey", LongType(), True),
        StructField("o_orderstatus", StringType(), True),
        StructField("o_totalprice", DecimalType(12, 2), True),
        StructField("o_orderdate", DateType(), True),
        StructField("o_orderpriority", StringType(), True),
        StructField("o_clerk", StringType(), True),
        StructField("o_shippriority", IntegerType(), True),
        StructField("o_comment", StringType(), True)
    ]),
    "part": StructType([
        StructField("p_partkey", LongType(), True),
        StructField("p_name", StringType(), True),
        StructField("p_mfgr", StringType(), True),
        StructField("p_brand", StringType(), True),
        StructField("p_type", StringType(), True),
        StructField("p_size", IntegerType(), True),
        StructField("p_container", StringType(), True),
        StructField("p_retailprice", DecimalType(12, 2), True),
        StructField("p_comment", StringType(), True)
    ]),
    "partsupp": StructType([
        StructField("ps_partkey", LongType(), True),
        StructField("ps_suppkey", LongType(), True),
        StructField("ps_availqty", IntegerType(), True),
        StructField("ps_supplycost", DecimalType(12, 2), True),
        StructField("ps_comment", StringType(), True)
    ]),
    "region": StructType([
        StructField("r_regionkey", LongType(), True),
        StructField("r_name", StringType(), True),
        StructField("r_comment", StringType(), True)
    ]),
    "supplier": StructType([
        StructField("s_suppkey", LongType(), True),
        StructField("s_name", StringType(), True),
        StructField("s_address", StringType(), True),
        StructField("s_nationkey", LongType(), True),
        StructField("s_phone", StringType(), True),
        StructField("s_acctbal", DecimalType(12, 2), True),
        StructField("s_comment", StringType(), True)
    ])
}

# --- 2. Initialize Contexts ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 3. Define Parameters ---
scale_factor = args['scale_factor']
sf = int(scale_factor)

target_s3_folder = args['target_s3_folder']

raw_data_base_path = args['raw_data_base_path']
athena_base_path = args['athena_base_path']
if not athena_base_path.endswith('/'):
    athena_base_path += '/'
if not raw_data_base_path.endswith('/'):
    raw_data_base_path += '/'

print(f"--- Starting conversion job (V4 - Fixed date types) ---")
print(f"--- Reading source: {raw_data_base_path}sf-{scale_factor}/")
print(f"--- Writing target: {athena_base_path}{target_s3_folder}/ (SF={sf}) ---")

# --- 4. Loop Processing for Each Table ---
for table_name, schema in schemas.items():

    s3_input_path = f"{raw_data_base_path}sf-{sf}/{table_name}.tbl"
    s3_output_path = f"{athena_base_path}{target_s3_folder}/{table_name}/"

    print(f"Processing table: {table_name}")

    input_dataframe = spark.read \
        .format("csv") \
        .schema(schema) \
        .option("sep", "|") \
        .option("dateFormat", "yyyy-MM-dd") \
        .load(s3_input_path)

    # --- 5. Dynamic Repartitioning (to solve small files problem) ---
    num_partitions = 1

    if table_name in ['nation', 'region']:
        num_partitions = 1
    elif table_name in ['supplier', 'part']:
        num_partitions = max(1, int(sf / 10))
    elif table_name in ['customer', 'partsupp']:
        num_partitions = max(1, int(sf / 5))
    elif table_name in ['orders', 'lineitem']:
        num_partitions = max(1, int(sf * 2))

    print(f"-> Repartitioning to {num_partitions} files...")
    final_dataframe = input_dataframe.repartition(num_partitions)

    # 6. Write Spark DataFrame to Parquet
    final_dataframe.write \
        .format("parquet") \
        .option("compression", "none") \
        .mode("overwrite") \
        .save(s3_output_path)

    print(f"Completed: {table_name}")

# --- Job Complete ---
job.commit()
print(f"--- Job {args['JOB_NAME']} has completed successfully ---")
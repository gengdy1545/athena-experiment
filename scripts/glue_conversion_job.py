import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- 必备：获取作业参数 ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_sf_prefix',
    'target_s3_folder'
])

# --- 初始化上下文 ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. 定义参数 ---
source_database = "tpch_raw_staging"
source_sf_prefix = args['source_sf_prefix']
target_s3_folder = args['target_s3_folder']
athena_base_path = "s3://home-dongyang/athena/"

# TPC-H 的 8 个标准表
tables_to_convert = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier'
]

print(f"--- 开始转换作业 ---")
print(f"--- 读取源: {source_database} (表前缀: {source_sf_prefix})")
print(f"--- 写入目标: {athena_base_path}{target_s3_folder}/ ---")

# --- 2. 循环处理每个表 ---
for table_name in tables_to_convert:

    # 爬网程序会根据 S3 路径自动命名表
    # e.g., s3://.../staging-tpch-raw/sf-10/ -> "sf_10_customer", "sf_10_lineitem"
    catalog_table_name = f"{source_sf_prefix}_{table_name}_tbl"

    # 最终输出的 S3 路径
    # e.g., s3://home-dongyang/athena/tpch_0/customer/
    output_path = f"{athena_base_path}{target_s3_folder}/{table_name}/"

    print(f"正在处理表: {catalog_table_name} -> {output_path}")

    # 1. 从 Glue 目录读取原始数据
    input_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database = source_database,
        table_name = catalog_table_name,
        transformation_ctx = f"input_{table_name}"
    )

    # 2. 转换为 Parquet 并写入 S3
    glueContext.write_dynamic_frame.from_options(
        frame = input_dynamic_frame,
        connection_type = "s3",
        connection_options = {
            "path": output_path
        },
        format = "parquet",
        format_options = {
            # --- 这是满足您要求的关键 ---
            "compression": "none"
            # -------------------------------
        },
        transformation_ctx = f"output_{table_name}"
    )

    print(f"完成: {table_name}")

# --- 作业完成 ---
job.commit()
print(f"--- 作业 {args['JOB_NAME']} 已成功完成 ---")
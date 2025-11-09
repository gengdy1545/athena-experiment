#!/bin/bash

# *****************************************************
# 此脚本用于在 EC2 实例上编译 TPC-H 数据生成器 (dbgen)，
# 生成原始的 .tbl (CSV) 文件，并将它们上传到一个 S3 暂存桶。
# *****************************************************

set -e
echo "--- 开始 TPC-H 原始数据生成 ---"

# 暂存 S3 桶 (所有 .tbl 文件将上传到这里)
STAGING_BUCKET="s3://home-dongyang/staging-tpch-raw"

echo "--- 正在克隆并构建 TPC-H dbgen ---"
git clone git@github.com:electrum/tpch-dbgen.git
cd tpch-dbgen
# 注意：TPC-H 默认使用 C90 标准，在高版本 GCC 中可能需要调整
# 如果 'make' 失败，请尝试修改 Makefile： CFLAGS = $(CDEF) -Wno-error=implicit-function-declaration
make

# 定义生成和上传的函数
# 参数 1 ($1): 比例因子 (Scale Factor, e.g., 10)
# 参数 2 ($2): S3 文件夹名称 (e.g., sf-10)
generate_and_upload() {
    local SF=$1
    local S3_FOLDER=$2
    local TARGET_S3_PATH="${STAGING_BUCKET}/${S3_FOLDER}/"

    echo "--- 正在为 SF=${SF} 生成数据... ---"

    # 清理旧文件 (如果存在)
    rm -f *.tbl

    # 运行 dbgen
    # -s $SF 指定比例因子 (大小)
    ./dbgen -s $SF

    echo "--- 数据生成完毕 (SF=${SF})。正在上传到 ${TARGET_S3_PATH} ---"

    # 使用 AWS CLI 将所有 .tbl 文件同步到 S3
    aws s3 sync . ${TARGET_S3_PATH} --exclude "*" --include "*.tbl"

    echo "--- SF=${SF} 上传完成 ---"
    rm -f *.tbl
}

# 4. 运行所有需要的规模 10G (SF-10), 30G (SF-30), 和 100G (SF-100)

generate_and_upload 10 "sf-10"

generate_and_upload 30 "sf-30"

generate_and_upload 100 "sf-100"

echo "--- 所有 TPC-H 原始数据已生成并上传到 S3 暂存区 ---"
#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  warehouse.sh
#    CreateTime    ：  2023-02-24 01:43
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  warehouse.sh 被用于 ==> 数仓中每层之间的计算
# =========================================================================================

PROJECT_DIR=$(cd "$(dirname "$0")/../" || exit; pwd)       # 项目根路径
LOG_FILE="warehouse-$(date +%F).log"                       # 操作日志

# 1. 创建日志目录和日志文件
mkdir -p "${PROJECT_DIR}/logs"
touch "${PROJECT_DIR}/logs/${LOG_FILE}"

# 2. HDFS ----> ODS
echo "======================================= HDFS -----> ODS ========================================"
"${PROJECT_DIR}/warehouse/hdfs-ods-log.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1
"${PROJECT_DIR}/warehouse/hdfs-ods-db.sh"  >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 3. ODS ----> DWD
echo "======================================== ODS -----> DWD ========================================"
"${PROJECT_DIR}/warehouse/ods-dwd.sh"      >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 4. DWD ----> DWS
echo "======================================== DWD ----> DWS ========================================"
"${PROJECT_DIR}/warehouse/dwd-dws-log.sh"  >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 5. DWS ----> ADS
echo "======================================== DWS ----> ADS ========================================"
"${PROJECT_DIR}/warehouse/dws-ads.sh"      >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "======================================= ADS ----> Mysql ======================================="
"${PROJECT_DIR}/hdfs-mysql/hdfs-mysql.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "========================================== 完成退出 ==========================================="
exit 0

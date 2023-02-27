#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  init.sh
#    CreateTime    ：  2023-02-24 01:42
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  init.sh 被用于 ==> 部署完成后，一键初始化
# =========================================================================================
    
    
SERVICE_DIR=$(cd "$(dirname "$0")/../" || exit; pwd)       # 程序位置
PROJECT_DIR=$(cd "${SERVICE_DIR}/../"  || exit; pwd)       # 项目根路径
MYSQL_HOME="/opt/db/mysql"                                 # Mysql 安装路径
HIVE_HOME="/opt/apache/hive"                               # Hive 安装路径

LOG_FILE="init-$(date +%F).log"                            # 操作日志
USER=$(whoami)                                             # 当前用户
HOST_LIST=(master slaver1 slaver2 slaver3)                 # 集群主机名称


# 1. 创建各个模块的日志目录
module_list=$(ls "${PROJECT_DIR}")
for module in "${module_list[@]}"
do
    mkdir -p "${PROJECT_DIR}/${module}/logs"
done

# 2. 在 Mysql 中，将 mock-db 的数据导入到 数据库 at_gui_gu
echo "****************************** 将 mock-db 的 sql 执行到数据库 ******************************"
${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111  < "${PROJECT_DIR}/mock-db/table.sql" || exit >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111  < "${PROJECT_DIR}/mock-db/data.sql"  || exit >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 3. 在 Hive 中创建所有的 ODS、DIM、DWD、DWS、ADS 表
echo "****************************** 在 Hive 中创建数据仓库各层的表 ******************************"
"${HIVE_HOME}/bin/hive" -f "${PROJECT_DIR}/sql/hive.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 4. 模拟生成用户行为日志历史日志
MAX_COUNT=10                                               # 模拟数据生成循环次数


# 4. 将用户行为日志导出到 kafka
echo "***************************** 将 mock-log 中的日志导出到 kafka *****************************"
"${PROJECT_DIR}/file-kafka/file-kafka.sh start"          >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 5. 将 Mysql 中的 维表数据 通过 DataX 同步到 hdfs
echo "***************************** 将 Mysql 的 维表数据 同步到 hdfs *****************************"
"${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start"          >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 6. 将 Mysql 中的 实时数据 通过 maxwell 同步到 kafka
echo "***************************** 将 Mysql 的 实时数据 同步到 hdfs *****************************"
"${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start"          >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 7. 将 kafka 中的 用户行为日志 和 业务实时数据 同步到 hdfs
echo "******************************* 将 kafka 的 数据 同步到 hdfs *******************************"
"${PROJECT_DIR}/mysql-hdfs/kafka-hdfs-log.sh start"          >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
"${PROJECT_DIR}/mysql-hdfs/kafka-hdfs-db.sh start"          >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 8. 将 HDFS 的数据加载到 Hive 的 ODS
echo "****************************** 将同步的 HDFS 数据加载到 Hive *******************************"


# 2. HDFS ----> ODS
echo "======================================= HDFS -----> ODS ========================================"
"${PROJECT_DIR}/warehouse/hdfs-ods-log.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
"${PROJECT_DIR}/warehouse/hdfs-ods-db.sh"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 3. ODS ----> DWD
echo "======================================== ODS -----> DWD ========================================"
"${PROJECT_DIR}/warehouse/ods-dwd-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 4. DWD ----> DWS
echo "======================================== DWD ----> DWS ========================================"
"${PROJECT_DIR}/warehouse/dwd-dws-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 5. DWS ----> ADS
echo "======================================== DWS ----> ADS ========================================"
"${PROJECT_DIR}/warehouse/dws-ads-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "======================================= ADS ----> Mysql ======================================="
"${PROJECT_DIR}/hdfs-mysql/hdfs-mysql.sh"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "========================================== 完成退出 ==========================================="
exit 0

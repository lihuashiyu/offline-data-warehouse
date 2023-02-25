#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  init.sh
#    CreateTime    ：  2023-02-24 01:42
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  init.sh 被用于 ==> 部署完成后，一键初始化
# =========================================================================================
    
    
PROJECT_DIR=$(cd "$(dirname "$0")/../" || exit; pwd)       # 项目根路径
MYSQL_HOME="/opt/db/mysql"                                 # Mysql 安装路径
HIVE_HOME="/opt/apache/hive"                               # Hive 安装路径

LOG_FILE="init-$(date +%F).log"                            # 操作日志
USER=$(whoami)                                             # 当前用户
HOST_LIST=(master slaver1 slaver2 slaver3)                 # 集群主机名称


# 1. 创建日志目录和日志文件
mkdir -p "${PROJECT_DIR}/logs"
touch "${PROJECT_DIR}/logs/${LOG_FILE}"

# 2. 在 Mysql 中，将 mock-db 的数据导入到 数据库 at_gui_gu
echo "****************************** 将 mock-db 的 sql 执行到数据库 ******************************"
${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111  < "${PROJECT_DIR}/mock-db/table.sql" || exit >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1
${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111  < "${PROJECT_DIR}/mock-db/data.sql"  || exit >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 3. 在 Hive 中创建所有的 ODS、DIM、DWD、DWS、ADS 表
"${HIVE_HOME}/bin/hive" -f "${PROJECT_DIR}/sql/hive.sql" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 4. 将用户行为日志导出到 kafka   
# "${PROJECT_DIR}/file-hdfs/mysql-hdfs.sh start"          >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 5. 将 Mysql 中的 维表数据 通过 DataX 同步到 hdfs
"${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start"          >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 6. 将 Mysql 中的 实时数据 通过 maxwell 同步到 kafka
"${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start"          >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 7. 将 kafka 中的 用户行为日志 和 业务实时数据 同步到 hdfs
"${PROJECT_DIR}/mysql-hdfs/kafka-hdfs-log.sh start"          >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1
"${PROJECT_DIR}/mysql-hdfs/kafka-hdfs-db.sh start"          >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 8. 将 HDFS 的数据加载到 Hive 的 ODS

# 2. HDFS ----> ODS
echo "======================================= HDFS -----> ODS ========================================"
"${PROJECT_DIR}/warehouse/hdfs-ods-log.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1
"${PROJECT_DIR}/warehouse/hdfs-ods-db.sh"  >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 3. ODS ----> DWD
echo "======================================== ODS -----> DWD ========================================"
"${PROJECT_DIR}/warehouse/ods-dwd-init.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 4. DWD ----> DWS
echo "======================================== DWD ----> DWS ========================================"
"${PROJECT_DIR}/warehouse/dwd-dws-init.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 5. DWS ----> ADS
echo "======================================== DWS ----> ADS ========================================"
"${PROJECT_DIR}/warehouse/dws-ads-init.sh" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "======================================= ADS ----> Mysql ======================================="
"${PROJECT_DIR}/hdfs-mysql/hdfs-mysql.sh"  >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 6. ADS ----> Mysql
echo "========================================== 完成退出 ==========================================="
exit 0

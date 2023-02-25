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
"${MYSQL_HOME}/bin/hive" -f "${PROJECT_DIR}/sql/hive.sql" >> "${PROJECT_DIR}/logs/${LOG_FILE}" 2>&1

# 4. 将 Mysql 中的 维表数据 通过 DataX 同步到 hdfs
"${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start" 


# 2. 遍历循环读取主机 ${HOST_LIST[@]}
for host_name in "${HOST_LIST[@]}"
do
    
    echo "============================== 向主机（${host_name}）同步数据 =============================="    
    ssh "${USER}@${host_name}" "mkdir -p ${PROJECT_DIR}; exit "
    
    # 执行同步
    rsync -zav --delete  "${PROJECT_DIR}"  "${USER}@${host_name}:${PROJECT_DIR}"
done

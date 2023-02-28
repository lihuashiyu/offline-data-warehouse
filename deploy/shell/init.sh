#!/usr/bin/env bash
# shellcheck disable=SC2317

# =========================================================================================
#    FileName      ：  init.sh
#    CreateTime    ：  2023-02-24 01:42
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  init.sh 被用于 ==> 部署完成后，一键初始化
# =========================================================================================
    
    
SERVICE_DIR=$(cd "$(dirname "$0")"    || exit; pwd)        # 程序位置
PROJECT_DIR=$(cd "${SERVICE_DIR}/../" || exit; pwd)        # 项目根路径
MYSQL_HOME="/opt/db/mysql"                                 # Mysql 安装路径
HIVE_HOME="/opt/apache/hive"                               # Hive 安装路径

LOG_FILE="init-$(date +%F).log"                            # 操作日志
USER=$(whoami)                                             # 当前用户
HOST_LIST=(master slaver1 slaver2 slaver3)                 # 集群主机名称
MASTER_LIST=(master)                                       # master 主机名称
SLAVER_LIST=(slaver1 slaver2 slaver3)                      # slaver 主机名称


# 1. 创建各个模块的日志目录
function create_model_log()
{
    module_list=$(ls -d "${PROJECT_DIR}"/*/)
    for module in ${module_list}
    do
        for host_name in "${HOST_LIST[@]}"
        do
            ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; mkdir -p ${module}/logs"
        done
    done
}

# 2. 创建所有 mysql 和 hive 表
function create_table()
{
    # 1.创建数据库并授权
    atguigu="    drop database if exists at_gui_gu;   create database if not exists at_gui_gu;   grant all privileges on at_gui_gu.* to 'issac'@'%';   flush privileges;"
    view_report="drop database if exists view_report; create database if not exists view_report; grant all privileges on view_report.* to 'issac'@'%'; flush privileges;"
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uroot -p111111 -e "${atguigu}"    >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uroot -p111111 -e "${view_report}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 2. 在 Mysql 中，将 mock-db 的数据导入到 数据库 at_gui_gu
    echo "****************************** 将 mock-db 的 sql 执行到数据库 ******************************"
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111 -Dat_gui_gu < "${PROJECT_DIR}/mock-db/table.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111 -Dat_gui_gu < "${PROJECT_DIR}/mock-db/data.sql"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

    # 3. 在 Hive 中创建所有的 ODS、DIM、DWD、DWS、ADS 表
    echo "****************************** 在 Hive 中创建数据仓库各层的表 ******************************"
    "${HIVE_HOME}/bin/hive" -f "${PROJECT_DIR}/sql/hive.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 4. 在 Mysql 中创建所有从 Hive 中导出的表结构
    echo "****************************** 在 Mysql 中创建 ADS 层的映射表 ******************************"
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111 -Dview_report < "${PROJECT_DIR}/sql/export.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}

# 3. 模拟生成 5 天的 用户行为 历史日志
function generate_log()
{
    echo "****************************** 模拟生成 5 天的用户行为日志 ******************************"
    number=5
        
    while [ "${number}" -gt 0 ]
    do
        nd_date=$(date "+%Y-%m-%d" -d "-${number} days")
        number=$((number - 1))
        
        for host_name in "${SLAVER_LIST[@]}"
        do
            echo "    ************************** ${host_name} ： ${nd_date} **************************    "
            ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mock-log/cycle.sh ${nd_date}; mv ${PROJECT_DIR}/mock-log/logs/mock-$(date +%F).log ${PROJECT_DIR}/mock-log/logs/mock-${nd_date}.log "
        done
    done
}

# 4. 模拟生成 5 天的 业务数据 历史数据
function generate_db()
{
    echo "****************************** 模拟生成 5 天的业务数据 ******************************"
    number=5
    while [ "${number}" -gt 0 ]
    do
        nd_date=$(date "+%Y-%m-%d" -d "-${number} days")
        
        for host_name in "${MASTER_LIST[@]}"
        do
            if [ "${number}" == "5" ]; then
                echo "    在主机（${host_name}）初始化数据库    "
                ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; cd ${PROJECT_DIR}/mock-db/ || exit; java -jar mock-db.jar >> logs/init.log 2>&1 "
                sleep 30
            fi
            ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mock-db/cycle.sh ${nd_date}"
        done
        number=$((number - 1))
    done
}

# 5. 将 Mysql 中的 维表数据 通过 DataX 同步到 hdfs
function mysql_hdfs()
{
    echo "***************************** 将 Mysql 的 维表数据 同步到 hdfs *****************************"
    for host_name in "${MASTER_LIST[@]}"
    do
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh start" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 6. 将 Mysql 中的历史 实时数据 通过 maxwell 同步到 kafka
function mysql_kafka()
{
    echo "***************************** 将 Mysql 的 实时数据 同步到 kafka *****************************"
    
    ${MYSQL_HOME}/bin/mysql -hmaster -P3306 -uissac -p111111 < "${PROJECT_DIR}/mysql-kafka/meta.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    for host_name in "${MASTER_LIST[@]}"
    do
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mysql-kafka/mysql-kafka-init.sh start" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 7. 将 kafka 中的 用户行为日志 和 业务实时数据 同步到 hdfs
function kafka_hdfs()
{
    echo "******************************* 将 kafka 的 数据 同步到 hdfs *******************************"
    for host_name in "${MASTER_LIST[@]}"
    do
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-log.sh start" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-db.sh start" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 8. 将 HDFS 的数据加载到 Hive 的 ODS
function warehouse()
{
    # 1. HDFS ----> ODS
    echo "======================================= HDFS -----> ODS ========================================"
    "${PROJECT_DIR}/warehouse/hdfs-ods-log.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    "${PROJECT_DIR}/warehouse/hdfs-ods-db.sh"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 2. ODS ----> DWD
    echo "======================================== ODS -----> DWD ========================================"
    "${PROJECT_DIR}/warehouse/ods-dwd-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 3. DWD ----> DWS
    echo "======================================== DWD ----> DWS ========================================"
    "${PROJECT_DIR}/warehouse/dwd-dws-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 4. DWS ----> ADS
    echo "======================================== DWS ----> ADS ========================================"
    "${PROJECT_DIR}/warehouse/dws-ads-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 5. ADS ----> Mysql
    echo "======================================= ADS ----> Mysql ======================================="
    "${PROJECT_DIR}/hdfs-mysql/hdfs-mysql.sh"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 6. ADS ----> Mysql
    echo "========================================== 完成退出 ==========================================="
}


create_model_log
create_table
generate_log
kafka_hdfs
mysql_kafka
generate_db
mysql_hdfs

exit 0

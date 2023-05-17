#!/usr/bin/env bash

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

MYSQL_HOST="master"                                        # Mysql 安装的节点
HIVE_HOST="master"                                         # Hive  安装的节点
MOCK_LOG_HOST_LIST=(slaver1 slaver2 slaver3)               # 需要生成 历史 行为日志 的节点
MOCK_DB_HOST_LIST=(slaver1 slaver2 slaver3)                # 需要生成 历史 业务数据 的节点
DATAX_HOST_LIST=(master)                                   # 同步 数据库中的 全量数据 到 HDFS 使用的 Datax 所在的节点 
MAXWELL_HOST_LIST=(slaver1)                                # 同步生成的数据库中的 历史增量数据 到 Kafka 使用的 MaxWell 所在的节点 
KAFKA_LOG_HOST_LIST=(slaver2)                              # 将 Kafka 中的历史日志同步到 HDFS 的 Flume 所在的节点
KAFKA_DB_HOST_LIST=(slaver3)                               # 将 Kafka 中的增量数据同步到 HDFS 的 Flume 所在的节点

USER=$(whoami)                                             # 当前用户
LOG_FILE="init-$(date +%F).log"                            # 操作日志


# ============================================= 定义函数 ============================================== 
# 1. 创建各个模块的日志目录
function create_model_log()
{
    module_list=$(ls -d "${PROJECT_DIR}"/*/)
    
    for module in ${module_list}
    do
        echo "    在目录（${module}）中创建 日志目录 logs "
        mkdir -p "${module}/logs"
    done
}

# 2. 创建所有 mysql 和 hive 表
function create_table()
{
    # 2.1创建数据库并授权
    atguigu="    drop database if exists at_gui_gu;   create database if not exists at_gui_gu;   grant all privileges on at_gui_gu.*   to 'issac'@'%'; flush privileges;"
    view_report="drop database if exists view_report; create database if not exists view_report; grant all privileges on view_report.* to 'issac'@'%'; flush privileges;"
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uroot -p111111 -e "${atguigu}"     >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uroot -p111111 -e "${view_report}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 2.2 在 Mysql 中，将 mock-db 的数据导入到 数据库 at_gui_gu
    echo "****************************** 将 mock-db 的 sql 执行到数据库 ******************************"
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uissac -p111111 -Dat_gui_gu < "${PROJECT_DIR}/mock-db/table.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uissac -p111111 -Dat_gui_gu < "${PROJECT_DIR}/mock-db/data.sql"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 2.3 在 Hive 中创建所有的 ODS、DIM、DWD、DWS、ADS 表
    echo "****************************** 在 Hive 中创建数据仓库各层的表 ******************************"
    "${HIVE_HOME}/bin/hive" -h ${HIVE_HOST} -p 10000 -f "${PROJECT_DIR}/sql/hive.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 2.4 在 Mysql 中创建所有从 Hive 中导出的表结构
    echo "****************************** 在 Mysql 中创建 ADS 层的映射表 ******************************"
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uissac -p111111 -Dview_report < "${PROJECT_DIR}/sql/export.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
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
        
        for host_name in "${MOCK_LOG_HOST_LIST[@]}"
        do
            echo "    ************************** ${host_name} ：${nd_date} **************************    "
            ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mock-log/cycle.sh ${nd_date}; mv ${PROJECT_DIR}/mock-log/logs/mock-$(date +%F).log ${PROJECT_DIR}/mock-log/logs/mock-${nd_date}.log "
        done
    done
}

# 4. 监控本地 用户行为日志 并同步到 Kafka
function log_monitor()
{
    echo "****************************** 监控行为日志并同步到 Kafka ******************************"
    for host_name in "${MOCK_LOG_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/file-kafka/file-kafka.sh $1" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 5. 模拟生成 5 天的 业务历史数据
function generate_db()
{
    echo "****************************** 模拟生成 5 天的业务数据 ******************************"
    # 5.1 定义 5 天
    number=5
    while [ "${number}" -gt 0 ]
    do
        # 5.2 获取日期
        nd_date=$(date "+%Y-%m-%d" -d "-${number} days")
        
        # 5.3 在各节点进行生成数据
        for host_name in "${MOCK_DB_HOST_LIST[@]}"
        do
            echo "    ************************** ${host_name} ：${nd_date} **************************    "
            
            # 5.4 判断是否是初次生成数据
            if [ "${number}" == "5" ]; then
                echo "    在主机（${host_name}）初始化数据库    "
                ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; cd ${PROJECT_DIR}/mock-db/ || exit; java -jar mock-db.jar >> ${PROJECT_DIR}/mock-db/logs/init.log 2>&1 "
                sleep 30
            fi
            
            # 5.5 非初次执行时，循环生成
            ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mock-db/cycle.sh ${nd_date}"
        done
        number=$((number - 1))
    done
}

# 6. 监控数据库 实时业务数据 并同步到 Kafka
function db_monitor()
{
    echo "****************************** 监控业务数据并同步到 Kafka ******************************"
    for host_name in "${MAXWELL_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mysql-hdfs/mysql-kafka.sh $1" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done    
}

# 7. 将 Mysql 中的 维表数据 通过 DataX 全量同步到 hdfs
function mysql_hdfs()
{
    echo "***************************** 将 Mysql 的 维表数据 同步到 hdfs *****************************"
    for host_name in "${DATAX_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mysql-hdfs/mysql-hdfs.sh all" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 8. 将 Mysql 中的历史 实时数据 通过 maxwell 全量同步到 kafka
function mysql_kafka()
{
    # 8.1 初始化 MaxWell 元数据
    echo "***************************** 将 maxwell 的源数据导入到 Mysql *****************************"
    ${MYSQL_HOME}/bin/mysql -h${MYSQL_HOST} -P3306 -uissac -p111111 < "${PROJECT_DIR}/mysql-kafka/meta.sql" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 8.2 同步所有全量历史数据
    echo "***************************** 将 Mysql 的 全量数据 同步到 kafka *****************************"
    for host_name in "${MAXWELL_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/mysql-kafka/mysql-kafka-init.sh all" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done    
}

# 9. 将 kafka 中的 用户行为日志 和 业务实时数据 同步到 hdfs
function kafka_hdfs()
{
    # 9.1 将 Kafka 中的 用户行为日志 同步到 HDFS
    echo "******************************* 将 kafka 的 数据 同步到 hdfs *******************************"
    for host_name in "${KAFKA_LOG_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-log.sh $1" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done    
    
    # 9.2 将 Kafka 中的 业务实时数据 同步到 HDFS
    echo "******************************* 将 kafka 的 数据 同步到 hdfs *******************************"
    for host_name in "${KAFKA_DB_HOST_LIST[@]}"
    do
        echo "    ************************** ${host_name} **************************    "
        ssh "${USER}@${host_name}" "source ~/.bashrc; source /etc/profile; ${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-db.sh $1" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    done
}

# 10. 将 HDFS 的数据加载到 Hive 的 ODS
function warehouse_init()
{
    # 10.1 HDFS ----> ODS
    echo "======================================= HDFS -----> ODS ========================================"
    "${PROJECT_DIR}/warehouse/hdfs-ods.sh"     >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 10.2 ODS ----> DWD
    echo "======================================== ODS -----> DWD ========================================"
    "${PROJECT_DIR}/warehouse/ods-dwd-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1

    # 10.3 ODS ----> DIM
    echo "======================================== ODS -----> DIM ========================================"
    "${PROJECT_DIR}/warehouse/ods-dim-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 10.4 DWD ----> DWS
    echo "======================================== DWD ----> DWS ========================================"
    "${PROJECT_DIR}/warehouse/dwd-dws-init.sh" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 10.5 DWS ----> ADS
    echo "======================================== DWS ----> ADS ========================================"
    "${PROJECT_DIR}/warehouse/dws-ads.sh"      >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    # 10.6 ADS ----> Mysql
    echo "======================================= ADS ----> Mysql ======================================="
    "${PROJECT_DIR}/hdfs-mysql/hdfs-mysql.sh"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    
    echo "========================================== 完成退出 ==========================================="
}


# ============================================= 执行函数 ============================================== 
# 0. 给所有 shell 脚本添加可执行权限
find "${PROJECT_DIR}/" -iname "*.sh" -type f -exec chmod u+x {} + 

# 1.创建日志存储目录
create_model_log

# 2. 将解压后的项目同步到其它节点
"${SERVICE_DIR}"/xync.sh "${PROJECT_DIR}/"

# 3. 启动大数据组件
"${SERVICE_DIR}/component.sh" start 

# 4. 创建 Mysql 和 Hive 表
create_table

# 5. 开启同步的监控工具
kafka_hdfs  start
log_monitor start
db_monitor  start

# 6. 生成数据
generate_log
generate_db

# 7. 全量同步数据库数据
mysql_kafka
mysql_hdfs

# 8. 初始化 Hive 历史数据
warehouse_init

exit 0

#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  component.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  component.sh 被用于 ==> 离线数仓的组件，Hadoop、Spark、Hive、
#                                              Zookeeper、Kafka、Mysql 的启停脚本  
# =========================================================================================


# 服务所在位置
PROJECT_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 脚本所在路径
ROOT_DIR=$(cd "${PROJECT_DIR}/../" || exit; pwd)           # 项目根路径
MYSQL_HOME="/opt/db/mysql"                                 # Mysql 安装路径
HADOOP_HOME="/opt/apache/hadoop"                           # Hadoop 安装路径
SPARK_HOME="/opt/apache/spark"                             # Spark 安装路径
HIVE_HOME="/opt/apache/hive"                               # Hive 安装路径
ZOOKEEPER_HOME="/opt/apache/zookeeper"                     # Zookeeper 安装路径
KAFKA_HOME="/opt/apache/kafka"                             # Kafka 安装路径

HOST_LIST=(master slaver1 slaver2 slaver3)                 # 集群主机
USER=$(whoami)                                             # 获取当前登录用户
LOG_FILE="component-$(date +%F).log"                       # 操作日志


# 组件启动的 java 进程
function service_status()
{
    # 1. 遍历所有的主机，执行命令
    for host_name in "${HOST_LIST[@]}"
    do
        echo "****************************** 在主机 ${host_name} 上启动的 java 进程 ******************************"
        
        ssh "${USER}@${host_name}" "source ~/.bashrc; source ~/.bash_profile; ${MYSQL_HOME}/bin/mysql.sh status"
        ssh "${USER}@${host_name}" "source ~/.bashrc; source ~/.bash_profile; jps -l | grep -v sun.tools.jps.Jps | sort -n"
    done
}


# 大数据组件的启动
function service_start()
{
    echo "****************************** 启动 Mysql **********************************"
    "${MYSQL_HOME}/bin/mysql.sh"         start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
    
    echo "****************************** 启动 Zookeeper ******************************"
    "${ZOOKEEPER_HOME}/bin/Zookeeper.sh" start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
    
    echo "****************************** 启动 Hadoop *********************************"
    "${HADOOP_HOME}/bin/hadoop.sh"       start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
     
    echo "****************************** 启动 Spark **********************************"
    "${SPARK_HOME}/bin/spark.sh"         start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
     
    echo "****************************** 启动 Hive ***********************************"
    "${HIVE_HOME}/bin/hive.sh"           start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
    
    echo "****************************** 启动 Kafka **********************************"
    "${KAFKA_HOME}/bin/kafka.sh"         start >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
}


# # 大数据组件的停止
function service_stop()
{
    echo "****************************** 停止 Kafka **********************************"
    "${KAFKA_HOME}/bin/kafka.sh"         stop   >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
        
    echo "****************************** 停止 Hive ***********************************"
    "${HIVE_HOME}/bin/hive.sh"           stop  >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
    
    echo "****************************** 停止 Spark **********************************"
    "${SPARK_HOME}/bin/spark.sh"         stop  >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
    
    echo "****************************** 停止 Hadoop *********************************"
    "${HADOOP_HOME}/bin/hadoop.sh"       stop  >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
        
    echo "****************************** 停止 Zookeeper ******************************"
    "${ZOOKEEPER_HOME}/bin/zookeeper.sh" stop  >> "${ROOT_DIR}/${LOG_FILE}" 2>&1
        
    echo "****************************** 停止 Mysql **********************************"
    "${MYSQL_HOME}/bin/mysql.sh"         stop >> "${ROOT_DIR}/${LOG_FILE}" 2>&1   
}


printf "\n"
# 1. 创建日志目录
mkdir -p "${ROOT_DIR}/logs"

# 2. 创建日志文件
touch "${ROOT_DIR}/logs/${LOG_FILE}"

#  匹配输入参数
case "$1" in
    #  1. 运行程序
    start)
        service_start
    ;;
    
    #  2. 停止
    stop)
        service_stop
    ;;
    
    #  3. 状态查询
    status)
        service_status
    ;;
    
    #  4. 其它情况
    *)
        echo "    脚本可传入一个参数，如下所示： "
        echo "        +-----------------------+ "
        echo "        | start | stop | status | "
        echo "        +-----------------------+ "
        echo "        |  start  ： 启动服务   | "
        echo "        |  stop   ： 关闭服务   | "
        echo "        |  status ： 查看状态   | "
        echo "        +-----------------------+ "
    ;;
esac
printf "\n"

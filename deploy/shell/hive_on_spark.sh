#!/usr/bin/env bash


# 服务所在位置
PROJECT_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 项目路径
MYSQL_DIR="/opt/mysql"                                     # Mysql 安装路径
HADOOP_DIR="/opt/apache/hadoop"                            # Hadoop 安装路径
SPARK_DIR="/opt/apache/spark"                              # Spark 安装路径
HIVE_DIR="/opt/apache/hive"                                # Hive 安装路径
ZOOKEEPER_DIR="/opt/apache/zookeeper"                      # Zookeeper 安装路径
KAFKA_DIR="/opt/apache/kafka"                              # Kafka 安装路径
LOG_FILE="$(date +%F-%H-%M-%S).log"                        # 操作日志

printf "\n"
#  匹配输入参数
case "$1" in
    #  1. 运行程序
    start)
        echo "****************************** 启动 Mysql **********************************"
        "${MYSQL_DIR}/bin/mysql.sh" start >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
        
        echo "****************************** 启动 Zookeeper ******************************"
        "${ZOOKEEPER_DIR}/bin/zookeeper.sh" start >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1

        echo "****************************** 启动 Hadoop *********************************"
        "${HADOOP_DIR}bin/hadoop.sh" start  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1

        echo "****************************** 启动 Spark **********************************"
        "${HADOOP_DIR}bin/spark.sh" start  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1

        echo "****************************** 启动 Hive ***********************************"
        "${HIVE_DIR}bin/hive.sh" start   >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
        
        echo "****************************** 启动 Kafka **********************************"
        "${KAFKA_DIR}bin/kafka.sh" start   >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
    ;;
    
      
    #  2. 停止
    stop)
        echo "****************************** 停止 Kafka **********************************"
        "${KAFKA_DIR}bin/kafka.sh" stop   >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
        
        echo "****************************** 停止 Hive ***********************************"
        "${HIVE_DIR}/bin/hive.sh" stop  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1

        echo "****************************** 停止 Spark **********************************"
        "${SPARK_DIR}/bin/spark.sh" stop  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1

        echo "****************************** 停止 Hadoop *********************************"
        "${HADOOP_DIR}/bin/hadoop.sh" stop  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
        
        echo "****************************** 停止 Zookeeper ******************************"
        "${ZOOKEEPER_DIR}/bin/zookeeper.sh" stop  >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
        
        echo "****************************** 停止 Mysql **********************************"
        "${MYSQL_DIR}/bin/mysql.sh" stop >> "${PROJECT_DIR}/${LOG_FILE}" 2>&1
    ;;
    
    #  3. 状态查询
    status)
        echo "****************************** 启动的进程如下： ******************************"
        jps -l | grep -v sun.tools.jps.Jps | grep -v intellij | sort -n
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

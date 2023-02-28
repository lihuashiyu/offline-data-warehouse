#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  data-sync.sh
#    CreateTime    ：  2023-02-24 01:43
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  data-sync 被用于 ==> 整个数据流向数仓的启停脚本
# =========================================================================================
    
    
PROJECT_DIR=$(cd "$(dirname "$0")/../" || exit; pwd)       # 项目根路径
LOG_FILE="warehouse-$(date +%F).log"                       # 操作日志
SLAVER_LIST=(slaver1 slaver2 slaver3)                      # 集群主机名称
USER=$(whoami)                                             # 当前用户
    
    
# 1. 服务状态检测
function service_status()
{
    for host_name in "${SLAVER_LIST[@]}"
    do
        # 1. 模拟用户行为日志
        p1=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-log/mock-log.sh status     | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p1}"
        
        # 2. 模拟业务数据生成
        p2=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-db/mock-db.sh status       | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p2}"
        
        # 3. flume 日志监控
        p3=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/file-kafka/file-kafka.sh status | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p3}"
    done
    
    # 4. maxwell 数据库监控
    p4=$(ssh "${USER}@slaver1" "${PROJECT_DIR}/mysql-kafka/mysql-kafka.sh status   | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p4}"
    
    # 5. flume 将 kafka 的用户行为日志同步到 hdfs
    p5=$(ssh "${USER}@slaver2" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-log.sh status | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p5}"
    
    # 6. flume 将 kafka 的业务数据同步到 hdfs
    p6=$(ssh "${USER}@slaver3" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-db.sh status  | grep -Ev '^$|====' ") 
    echo "    主机（${host_name}） ：${p6}"
}
    
# 2. 服务启动
function service_start()
{  
    # 1. flume 将 kafka 的业务数据同步到 hdfs
    p1=$(ssh "${USER}@slaver3" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-db.sh start  | grep -Ev '^$|====' ") 
    echo "    主机（${host_name}） ：${p1}"
    
    # 2. flume 将 kafka 的用户行为日志同步到 hdfs
    p2=$(ssh "${USER}@slaver2" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-log.sh start | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p2}"
    
    # 3. maxwell 数据库监控
    p3=$(ssh "${USER}@slaver1" "${PROJECT_DIR}/mysql-kafka/mysql-kafka.sh start   | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p3}"
    
    for host_name in "${SLAVER_LIST[@]}"
    do
        # 4. flume 日志监控
        p4=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/file-kafka/file-kafka.sh start | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p4}"
        
        # 5. 模拟用户行为日志
        p5=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-log/cycle.sh start        | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p5}"
        
        # 6. 模拟业务数据生成
        p6=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-log/cycle.sh start        | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p6}"
    done
}
    
# 3. 服务停止
function service_stop()
{
    for host_name in "${SLAVER_LIST[@]}"
    do
        # 1. 模拟用户行为日志
        p1=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-log/mock-log.sh stop     | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p1}"
        
        # 2. 模拟业务数据生成
        p2=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/mock-db/mock-db.sh stop       | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p2}"
        
        # 3. flume 日志监控
        p3=$(ssh "${USER}@${host_name}" "${PROJECT_DIR}/file-kafka/file-kafka.sh stop | grep -Ev '^$|====' ")
        echo "    主机（${host_name}） ：${p3}"
    done
            
    # 4. maxwell 数据库监控
    p4=$(ssh "${USER}@slaver1" "${PROJECT_DIR}/mysql-kafka/mysql-kafka.sh stop   | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p4}"
    
    # 5. flume 将 kafka 的用户行为日志同步到 hdfs
    p5=$(ssh "${USER}@slaver2" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-log.sh stop | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p5}"
    
    # 6. flume 将 kafka 的业务数据同步到 hdfs
    p6=$(ssh "${USER}@slaver3" "${PROJECT_DIR}/kafka-hdfs/kafka-hdfs-db.sh stop  | grep -Ev '^$|====' ")
    echo "    主机（${host_name}） ：${p6}" 
}
    
    
printf "\n================================================================================\n"
#  匹配输入参数
case "$1" in
    # 1. 运行程序
    start)
        service_start
    ;;
    
    # 2. 停止
    stop)
        service_stop
    ;;
    
    # 3. 停止
    restart)
        service_stop
        sleep 1
        service_start
    ;;
    
    # 4. 状态查询
    status)
        service_status
    ;;
    
    # 4. 其它情况
    *)
        echo "    脚本可传入一个参数，如下所示：                     "
        echo "        +-------------------------------------------+ "
        echo "        |   start  |  stop  |  restart  |  status   | "
        echo "        +-------------------------------------------+ "
        echo "        |         start      ：    启动服务         | "
        echo "        |         stop       ：    关闭服务         | "
        echo "        |         restart    ：    重启服务         | "
        echo "        |         status     ：    查看状态         | "
        echo "        +-------------------------------------------+ "
    ;;
esac
printf "================================================================================\n\n"
exit 0

#!/usr/bin/env bash
    
# =========================================================================================
#    FileName      ：  kafka-hdfs-db.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  kafka-hdfs-db.sh 被用于 ==> 使用 flume 将模拟生成的 业务数据，
#                                                       从 kafka 同步到 hdfs
# =========================================================================================
    
    
FLUME_NG=/opt/apache/flume                                 # flume 安装路径
FLUME_PORT=44444                                           # flume 占用端口号
SERVICE_DIR=$(cd $(dirname "$0") || exit; pwd)             # 需要执行的服务路径
ALIAS_NAME="Kafka：db -> Flume -> HDFS"                        # 程序别名
CONF_FILE=kafka-hdfs-db.conf                               # 配置文件

KAFKA_URL="slaver1:9092,slaver2:9092,slaver3:9092"         # Kafka 连接 url
KAFKA_TOPIC="cart_info,comment_info,coupon_use,favor_info,order_detail_activity,order_detail_coupon,order_detail,order_info,order_refund_info,order_status_log,payment_info,refund_payment,user_info"  # Kafka 主题
HDFS_PATH="/warehouse/db/%{topic}_inc/$(date +%F)"         # 监控数据源路径
INTERCEPTOR_JAR=flume-1.0.jar                              # Flume 拦截器 jar 包
INTERCEPTOR_NAME=interceptor.TimeInterceptor\$Builder      # Flume 拦截器 类名

# LOG_FILE=$(date +%F-%H-%M-%S).log                        # 操作日志存储领
LOG_FILE="kafka-hdfs-db-$(date +%F).log"                   # 操作日志存储
INFO_OUT_TYPE="INFO,console"                               # 
RUN_STATUS=1                                               # 运行状态
STOP_STATUS=0                                              # 停止状态


# 服务状态检测
function service_status()
{
    pid_count=$(ps -aux | grep -i "${USER}" | grep -i "${SERVICE_DIR}/${CONF_FILE}" | grep -v grep | grep -v "$0" | wc -l)
    
    if [ "${pid_count}" -eq 0 ]; then
        echo "${STOP_STATUS}"
    else
        echo "${RUN_STATUS}"
    fi
}

# 服务启动
function service_start()
{
    # 1. 复制拦截器的 jar 并替换 拦截器名称
    # cp -fpr "${SERVICE_DIR}/${INTERCEPTOR_JAR}" ${FLUME_NG}/lib >> "${SERVICE_DIR}/${LOG_FILE}" 2>&1
    sed -i "s#a3.sources.r3.interceptors.i1.type.*#a3.sources.r3.interceptors.i1.type = ${INTERCEPTOR_NAME}#g" "${SERVICE_DIR}/${CONF_FILE}"

    # 2. 替换 Kafka 连接的 url 和 topic
    sed -i "s#a3.sources.r3.kafka.bootstrap.servers.*#a3.sources.r3.kafka.bootstrap.servers = ${KAFKA_URL}#g" "${SERVICE_DIR}/${CONF_FILE}"
    sed -i "s#a3.sources.r3.kafka.topics.*#a3.sources.r3.kafka.topics = ${KAFKA_TOPIC}#g" "${SERVICE_DIR}/${CONF_FILE}"
    
    # 3. 替换缓存数据检查点
    sed -i "s#a3.channels.c3.dataDirs.*#a3.channels.c3.dataDirs = ${SERVICE_DIR}/data/db/#g" "${SERVICE_DIR}/${CONF_FILE}"
    sed -i "s#a3.channels.c3.checkpointDir.*#a3.channels.c3.checkpointDir = ${SERVICE_DIR}/check-point/db/#g" "${SERVICE_DIR}/${CONF_FILE}"
    
    # 4. 替换数据存储目标 HDFS 路径
    sed -i "s#a3.sinks.k3.hdfs.path.*#a3.sinks.k3.hdfs.path = ${HDFS_PATH}#g" "${SERVICE_DIR}/${CONF_FILE}"
       
    # 5. 统计正在运行程序的 pid 的个数
    status=$(service_status)
    
    # 6. 若程序运行状态为停止，则运行程序，否则打印程序正在运行
    if [ "${status}" == "${STOP_STATUS}" ]; then
        # 3. 加载程序，启动程序
        echo "    程序（${ALIAS_NAME}）正在加载中 ......"
        
        nohup ${FLUME_NG}/bin/flume-ng agent --conf "${FLUME_NG}/conf"                       \
                                             --name a3                                       \
                                             --conf-file "${SERVICE_DIR}/${CONF_FILE}"       \
                                             --classpath "${SERVICE_DIR}/${INTERCEPTOR_JAR}" \
                                             -Dflume.root.logger=${INFO_OUT_TYPE}            \
                                             >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1 &                                             
        sleep 2
        echo "    程序（${ALIAS_NAME}）启动验证中 ...... "
        sleep 3
        
        stat=$(service_status)
        if [ "${stat}" == "${RUN_STATUS}" ]; then
            echo "    程序（${ALIAS_NAME}）启动成功 ...... "
        else
            echo "    程序（${ALIAS_NAME}）启动失败 ...... "
        fi
    else
        echo "    程序（${ALIAS_NAME}）正在运行中 ...... "
    fi
}

# 服务停止
function service_stop()
{
    # 1. 统计正在运行程序的 pid 的个数
    status=$(service_status)
    
    # 2 判断程序状态
    if [ "${status}" == "${STOP_STATUS}" ]; then
        echo "    程序（${ALIAS_NAME}）的进程不存在，程序没有运行 ...... "
    # 3. 杀死进程，关闭程序
    else
        echo "    程序（${ALIAS_NAME}）正在停止 ......"
        temp=$(ps -aux | grep -i "${USER}" | grep -i "${SERVICE_DIR}/${CONF_FILE}" | grep -v grep | grep -v "$0" | awk '{print $2}' | xargs kill -15)
        
        sleep 2
        echo "    程序（${ALIAS_NAME}）停止验证中 ...... "
        sleep 3
        
        # 4. 若还未关闭，则强制杀死进程，关闭程序
        stat=$(service_status)
        
        if [ "${pid_count}" == "${RUN_STATUS}" ]; then
            tmp=$(ps -aux | grep "${USER}" | grep -i "${SERVICE_DIR}/${CONF_FILE}" | grep -v grep | grep -v "$0" | awk '{print $2}' | xargs kill -9)
        fi
        
        echo "    程序（${ALIAS_NAME}）已经停止成功 ......"
    fi
}

printf "\n=========================================================================\n"
#  匹配输入参数
case "$1" in
    # 1. 运行程序：running
    start)
        service_start
    ;;
    
    # 2. 停止
    stop)
        service_stop
    ;;
    
    # 3. 状态查询
    status)
        # 3.1 查看正在运行程序的 pid
        status=$(service_status)
        
        # 3.2 判断运行状态
        if [ "${status}" == "${STOP_STATUS}" ]; then
            echo "    程序（${ALIAS_NAME}）已经停止 ...... "
        elif [ "${status}" == "${RUN_STATUS}" ]; then
            echo "    程序（${ALIAS_NAME}）正在运行中 ...... "
        fi
    ;;
    
    # 4. 重启程序
    restart)
        service_stop
        sleep 1
        service_start
    ;;
    
    # 5. 其它情况
    *)
        echo "    脚本可传入一个参数，如下所示：                    "
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
printf "=========================================================================\n\n"
exit 0

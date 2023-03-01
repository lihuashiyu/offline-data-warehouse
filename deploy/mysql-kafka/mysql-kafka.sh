#!/usr/bin/env bash
    
# =========================================================================================
#    FileName      ：  mysql-hdfs.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  mysql-hdfs.sh 被用于 ==> 使用 MaxWell 监控 Mysql，用于将产生的 
#                                                   增量业务数据 同步到 kafka
# =========================================================================================
    
    
MAX_WELL_HOME=/opt/github/maxwell                          # MaxWell 安装路径
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
SERVICE_NAME=com.zendesk.maxwell.Maxwell                   # MaxWell jar 名字
ALIAS_NAME="Mysql -> MaxWell -> Kafka"                     # 程序别名
PROFILE=config.properties                                  # 配置文件
LOG_FILE="mysql-kafka-$(date +%F).log"                     # 操作日志存储
# LOG_FILE=$(date +%F-%H-%M-%S).log                        # 操作日志存储

USER=$(whoami)                                             # 服务运行用户
RUN_STATUS=1                                               # 服务运行状态
STOP_STATUS=0                                              # 服务停止状态


# 服务状态检测
function service_status()
{
    # 1. 获取 pid 个数
    pid_count=$(ps -aux | grep -i "${USER}" | grep -i "${SERVICE_NAME}" | grep "${SERVICE_DIR}/${PROFILE}" | grep -v grep  | grep -v "$0" | wc -l)
    
    # 2. 判断程序循行状态
    if [ "${pid_count}" -eq 1 ]; then
        echo "${RUN_STATUS}"
    elif [ "${pid_count}" -le 1 ]; then
        echo "${STOP_STATUS}"
    else
        echo "    查看程序是否有重复使用的状况 ......"
    fi
}

# 服务启动
function service_start()
{
    # 1. 统计正在运行程序的 pid 的个数
    pc=$(service_status)
    
    # 2. 判断程序的状态
    if [[ ${pc} -lt 1 ]]; then
        # 2.1 启动 MaxWell
        ${MAX_WELL_HOME}/bin/maxwell --config "${SERVICE_DIR}/${PROFILE}" \
                                     --daemon >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
        
        echo "    程序（${ALIAS_NAME}）正在启动中 ......"
        sleep 2 
        echo "    程序（${ALIAS_NAME}）启动验证中 ......"
        sleep 3 
        
        # 2.2 判断程序启动是否成功
        count=$(service_status)
        if [ "${count}" -eq 1 ]; then
            echo "    程序（${ALIAS_NAME}）启动成功 ......"
        else
            echo "    程序（${ALIAS_NAME}）启动失败 ......"
        fi
    else
        echo "    程序（${ALIAS_NAME}）正在运行中 ......"
    fi
}

# 服务停止
function service_stop()
{
    # 1 统计正在运行程序的 pid 的个数
    pc=$(service_status)
    if [ "${pc}" -eq 0 ]; then
        echo "    程序（${ALIAS_NAME}）进程不存在，未在运行 ......"
    else
        temp=$(ps -aux | grep -i "${USER}" | grep -i "${SERVICE_NAME}" | grep -i "${SERVICE_DIR}/${PROFILE}" | grep -v grep  | grep -v "$0" | awk '{print $2}' | xargs kill -15)
        echo "    程序（${ALIAS_NAME}）正在停止 ......"
        
        sleep 2
        echo "    程序（${ALIAS_NAME}）停止验证中 ......"
        sleep 3
        
        pcn=$(service_status)
        if [ "${pcn}" -gt 0 ]; then
           tmp=$(ps -aux | grep -i "${USER}" | grep -i "${SERVICE_NAME}" | grep -i "${SERVICE_DIR}/${PROFILE}" | grep -v grep  | grep -v "$0" | awk '{print $2}' | xargs kill -9) 
        fi 
        echo "    程序（${ALIAS_NAME}）已经停止 ......"
    fi
}


printf "\n================================================================================\n"
#  匹配输入参数
case $1 in
    # #  1. 启动程序
    start )
        service_start
    ;;
    
    # 2. 停止程序
    stop )
        service_stop
    ;;

    #  3. 状态查询
    status)
        # 3.1 统计正在运行程序的 pid 的个数
        pc=$(service_status)
        
        #  3.2 判断运行状态
        if [ "${pc}" == "${RUN_STATUS}" ]; then
            echo "    程序（${ALIAS_NAME}）正在运行中 ...... "
        elif [ "${pc}" == "${STOP_STATUS}" ]; then
            echo "    程序（${ALIAS_NAME}）已经停止 ...... "
        else
            echo "    程序（${ALIAS_NAME}）运行出错 ...... "
        fi
    ;;

    # 4. 重启程序
    restart )
       service_stop
       sleep 1
       service_start
    ;;

    # 5. 其它情况
    *)
        echo "    脚本可传入一个参数，如下所示：          "
        echo "        +---------------------------------+ "
        echo "        | start | stop | restart | status | "
        echo "        +---------------------------------+ "
        echo "        |      start    ：  启动服务      | "
        echo "        |      stop     ：  关闭服务      | "
        echo "        |      restart  ：  重启服务      | "
        echo "        |      status   ：  查看状态      | "
        echo "        +---------------------------------+ "
    ;;
esac
printf "================================================================================\n\n"
exit 0

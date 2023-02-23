#!/usr/bin/env bash

SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
# SERVICE_NAME=gmall2020-mock-log-2021-01-22.jar           
SERVICE_NAME=mock-log.jar                                  # 程序名称
PROFILE=application.yml                                    # 配置文件
PROGRAM_NAME=mock-log                                      # 程序别名
LOG_FILE=$(date +%F-%H-%M-%S).log                        # 操作日志存储
MOCK_DATE=2021-08-05


printf "\n=========================================================================\n"
#  匹配输入参数
case "$1" in
    #  1. 运行程序
    start)
        # 1.1 统计正在运行程序的 pid 的个数
        program_count=$(ps -aux | grep -v "$0" | grep ${SERVICE_NAME} | grep -v grep | wc -l)

        #  1.2 若 pid 个数为 0，则运行程序，否则打印程序正在运行
        if [ "${program_count}" -eq 0 ]; then
            # 配置日志路径
            sed -i "s#mock.date.*#mock.date: \"${MOCK_DATE}\"#g" "${SERVICE_DIR}/application.yml"
            sed -i "s#logging.config.*#logging.config: \"${SERVICE_DIR}/logback.xml\"#g" "${SERVICE_DIR}/application.yml"
            sed -i "s#<property name=\"LOG_HOME\" value=.*#<property name=\"LOG_HOME\" value=\"${SERVICE_DIR}/log\" />#g" "${SERVICE_DIR}/logback.xml"

            nohup java -jar "${SERVICE_DIR}/${SERVICE_NAME}" \
                       --spring.config.location="${SERVICE_DIR}/${PROFILE}" \
                       > /dev/null 2>&1 &
                       # > "${SERVICE_DIR}/log/${LOG_FILE}" 2>&1 &

            echo "    程序（${PROGRAM_NAME}）正在启动中 ......"
            sleep 5

            # 1.3 判断程序启动是否成功
            count=$(ps -aux | grep -v "$0" | grep ${SERVICE_NAME} | grep -v grep | wc -l)
            if [ "${count}" -eq 1 ]; then
                echo "    程序（${PROGRAM_NAME}）启动成功 ......"
            else
                echo "    程序（${PROGRAM_NAME}）启动失败 ......"
            fi
        else
            echo "    程序（${PROGRAM_NAME}）正在运行中 ......"
        fi
        ;;

    #  2. 停止
    stop)
        program_count=$(ps -aux | grep -v "$0" | grep ${SERVICE_NAME} | grep -v grep | wc -l)
        if [ "${program_count}" -eq 0 ]; then
            echo "    程序（${PROGRAM_NAME}）进程不存在，未在运行 ......"
        else
            echo "    程序（${PROGRAM_NAME}）正在停止 ......"
            p=$(ps -aux | grep -v "$0" | grep ${SERVICE_NAME} | grep -v grep | awk '{print $2}' | xargs kill -15)
            sleep 5
            echo "    程序（${PROGRAM_NAME}）已经停止 ......"
        fi
        ;;

    #  3. 状态查询
    status)
        # 3.1 统计正在运行程序的 pid 的个数
        program_count=$(ps -aux | grep -v "$0" | grep ${SERVICE_NAME} | grep -v grep | wc -l)

        #  3.2 判断运行状态
        if [ "${program_count}" -gt 0 ]; then
            echo "    程序（${PROGRAM_NAME}）正在启动中 ......"
        else
            echo "    程序（${PROGRAM_NAME}）已经停止 ......"
        fi
        ;;

    #  4. 重启程序
    restart)
        $0 stop
        $0 start
        ;;

    # 5. 其它情况
    *)
        echo "    脚本可传入一个参数，如下所示：                       "
        echo "        +--------------------------------------------+ "
        echo "        |  start | stop | restart | status | reload  | "
        echo "        +--------------------------------------------+ "
        echo "        |          start      ：    启动服务         | "
        echo "        |          stop       ：    关闭服务         | "
        echo "        |          restart    ：    重启服务         | "
        echo "        |          status     ：    查看状态         | "
        echo "        |          reload     ：    重新加载         | "
        echo "        +---------------------------------------------+ "
        ;;
esac
printf "=========================================================================\n\n"


#!/usr/bin/env bash


SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
rm -f "${SERVICE_DIR}/log/mock-*.log"                      # 删除日志
CONSTANT=1
number=0

while [ "${CONSTANT}" == "$CONSTANT" ]                     # 死循环启动
do
    number=$(( ${number} + 1 ))
    echo "============================== ${number} : $(date +%F-%H-%M-%S) =============================="
    # nohup "${SERVICE_DIR}/mock-log.sh" start >> "${SERVICE_DIR}/log/cycle.log" 2>&1 &
    nohup "${SERVICE_DIR}/mock-log.sh" start > /dev/null 2>&1 &
    sleep 20
done

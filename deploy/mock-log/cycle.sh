#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  cycle.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  cycle.sh 被用于 ==> 调用 mock-log.sh，进行循环生成，默认 10 次
# =========================================================================================
    
    
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
rm -f "${SERVICE_DIR}/log/mock-*.log"                      # 删除日志
CONSTANT=10
number=0

while [ "${CONSTANT}" == "$CONSTANT" ]                     # 死循环启动
do
    number=$(( ${number} + 1 ))
    echo "============================== ${number} : $(date +%F-%H-%M-%S) =============================="
    # nohup "${SERVICE_DIR}/mock-log.sh" start >> "${SERVICE_DIR}/log/cycle.log" 2>&1 &
    nohup "${SERVICE_DIR}/mock-log.sh" start > /dev/null 2>&1 &
    sleep 20
done

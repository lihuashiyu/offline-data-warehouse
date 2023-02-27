#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  cycle.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  cycle.sh 被用于 ==> 调用 mock-log.sh，进行循环生成，默认 10 次
# =========================================================================================
        
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
LOG_FILE="cycle-$(date +%F).log"                           # 程序运行日志文件
MAX_COUNT=10                                               # 模拟数据生成循环次数
number=0                                                   # 初始值

# 循环启动
while [ "${number}" -le "${MAX_COUNT}" ]
do
    number=$((number + 1))
    echo "    ****************************** ${number} : $(date +%F %H:%M:%S) ****************************** "
    "${SERVICE_DIR}/mock-log.sh" start >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1 
    sleep 20
done

exit 0

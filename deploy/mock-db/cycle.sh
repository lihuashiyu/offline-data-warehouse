#!/usr/bin/env bash


SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
# MOCK_DATE=$(date +%F)                                      # 生成业务数据的日期
MOCK_DATE=2021-08-15                                       # 生成业务数据的日期
rm -f "${SERVICE_DIR}/log/mock-*.log"                      # 删除日志
CONSTANT=1
number=0

echo "${MOCK_DATE}" >> "${SERVICE_DIR}/cycle.log" 2>&1

# 死循环启动
while [ "${CONSTANT}" == "$CONSTANT" ]
do
    number=$((number + 1))
    echo "============================== ${number} : $(date +%F-%H:%M:%S) =============================="
    "${SERVICE_DIR}/mock-db.sh" start "${MOCK_DATE}">> "${SERVICE_DIR}/cycle.log" 2>&1
    nohup "${SERVICE_DIR}/mock-db.sh" start "${MOCK_DATE}" > /dev/null 2>&1 &
    sleep 15
done


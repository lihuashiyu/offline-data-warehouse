#!/usr/bin/env bash


# 定义变量方便修改
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
MOCK=mock                                                  # 日志名称
WARE_HOUSE_DIR=/warehouse/origin                           # 日志在 HDFS 上的路径


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=$(date -d "-1 day" +%F)
fi

echo "======================================== 日志日期为 ${do_date} ========================================"
sql="load data inpath '${WARE_HOUSE_DIR}/${MOCK}/${do_date}' into table ${HIVE_DATA_BASE}.ods_log_inc partition(dt='${do_date}');"
hive -e "$sql"


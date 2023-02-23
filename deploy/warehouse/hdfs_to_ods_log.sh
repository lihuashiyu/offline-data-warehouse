#!/usr/bin/env bash


# 定义变量方便修改
APP=gmall
WARE_HOUSE_DIR=/user/warehouse                             # 仓库在 HDFS 上的路径


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=$(date -d "-1 day" +%F)
fi

echo "======================================== 日志日期为 ${do_date} ========================================"
sql="load data inpath '${WARE_HOUSE_DIR}/${APP}/log/${do_date}' into table ${APP}.ods_log partition(dt='${do_date}');"

import_comment_info
hive -e "$sql"

# hadoop jar /opt/Apache/Hadoop/share/hadoop/common/hadooplzo-0.4.20.jar \
#     com.hadoop.compression.lzo.DistributedLzoIndexer \
#     /user/warehouse/origin/$APP/log/dt=$do_date


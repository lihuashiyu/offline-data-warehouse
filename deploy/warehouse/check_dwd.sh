#!/usr/bin/env bash

DT=$1
[ "$DT" ] || DT=$(date -d '-1 day' +%F)

# 检查表 dwd_order_info 重复ID
# 参数： -t 表名
#       -d 日期
#       -c 检查重复值的列
#       -s 异常指标下限
#       -x 异常指标上限
#       -l 告警级别
bash duplicate.sh -t dwd_order_info -d "${DT}" -c id -s 0 -x 5 -l 0


# 检查表 dwd_order_info 的空ID
#参数： -t 表名
#       -d 日期
#       -c 检查空值的列
#       -s 异常指标下限
#       -x 异常指标上限
#       -l 告警级别
bash null_id.sh -t dwd_order_info -d "${DT}" -c id -s 0 -x 10 -l 0

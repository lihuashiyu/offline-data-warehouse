#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  hdfs-mysql.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  hdfs-mysql.sh 被用于 ==> 将 ADS 层数据导出到 Mysql  
# =========================================================================================
    
    
DATAX_DIR=/opt/github/datax                                # Datax 安装路径
HADOOP_DIR=/opt/apache/hadoop                              # Hadoop 路径
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
WAREHOUSE_DIR=/warehouse/ads                               # HDFS 的路径
MYSQL_DATA_BASE=view_report                                # Mysql 数据库名
LOG_FILE=hdfs_mysql.log                                    # 操作日志


# DataX 导出路径不允许存在空文件，该函数作用为清理空文件
function export_data()
{
    # 递归查询输入路径下的所有文件
    data_dir_list=$("${HADOOP_DIR}/bin/hadoop" fs -ls -R "$2" | awk '{print $8}')
    for path in ${data_dir_list}
    do
        # DataX导出路径不允许存在空文件，该函数作用为清理空文件
        "${HADOOP_DIR}/bin/hadoop" fs -test -z "${path}"
        if [[ $? -eq 0 ]]; then
            echo "    路径（${path}）中的文件大小为0，正在删除 ...... "
            "${HADOOP_DIR}/bin/hadoop" fs -rm -r -f "${path}"  >> "${SERVICE_DIR}/${LOG_FILE}" 2>&1
        fi
    done
    
    # 执行计划
    echo "    DataX 正在同步 HDFS（$2）的文件到 Mysql（${MYSQL_DATA_BASE}.$(echo $1 | awk -F '[/.]' '{print $4}')）数据库 ...... "
    /usr/bin/python3 "${DATAX_DIR}/bin/datax.py" -p "-Dexportdir=$2" "$1"  >> "${SERVICE_DIR}/${LOG_FILE}" 2>&1
}


printf "\n=================================== 运行开始 ===================================\n"
case $1 in
    "ads_new_buyer_stats")
        export_data "${SERVICE_DIR}/conf/ads_new_buyer_stats.json" "${WAREHOUSE_DIR}/ads_new_buyer_stats"
    ;;
    
    "ads_order_by_province")
        export_data "${SERVICE_DIR}/conf/ads_order_by_province.json" "${WAREHOUSE_DIR}/ads_order_by_province"
    ;;
    
    "ads_page_path")
        export_data "${SERVICE_DIR}/conf/ads_page_path.json" "${WAREHOUSE_DIR}/ads_page_path"
    ;;
    
    "ads_repeat_purchase_by_tm")
        export_data "${SERVICE_DIR}/conf/ads_repeat_purchase_by_tm.json" "${WAREHOUSE_DIR}/ads_repeat_purchase_by_tm"
    ;;
    
    "ads_trade_stats")
        export_data "${SERVICE_DIR}/conf/ads_trade_stats.json" "${WAREHOUSE_DIR}/ads_trade_stats"
    ;;
    
    "ads_trade_stats_by_cate")
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_cate.json" "${WAREHOUSE_DIR}/ads_trade_stats_by_cate"
    ;;
    
    "ads_trade_stats_by_tm")
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_tm.json" "${WAREHOUSE_DIR}/ads_trade_stats_by_tm"
    ;;
    
    "ads_traffic_stats_by_channel")
        export_data "${SERVICE_DIR}/conf/ads_traffic_stats_by_channel.json" "${WAREHOUSE_DIR}/ads_traffic_stats_by_channel"
    ;;
    
    "ads_user_action")
        export_data "${SERVICE_DIR}/conf/ads_user_action.json" "${WAREHOUSE_DIR}/ads_user_action"
    ;;
    
    "ads_user_change")
        export_data "${SERVICE_DIR}/conf/ads_user_change.json" "${WAREHOUSE_DIR}/ads_user_change"
    ;;
    
    "ads_user_retention")
        export_data "${SERVICE_DIR}/conf/ads_user_retention.json" "${WAREHOUSE_DIR}/ads_user_retention"
    ;;
    
    "ads_user_stats")
        export_data "${SERVICE_DIR}/conf/ads_user_stats.json" "${WAREHOUSE_DIR}/ads_user_stats"
    ;;
    
    "ads_activity_stats")
        export_data "${SERVICE_DIR}/conf/ads_activity_stats.json" "${WAREHOUSE_DIR}/ads_activity_stats"
    ;;
    
    "ads_coupon_stats")
        export_data "${SERVICE_DIR}/conf/ads_coupon_stats.json" "${WAREHOUSE_DIR}/ads_coupon_stats"
    ;;
    
    "ads_sku_cart_num_top3_by_cate")
        export_data "${SERVICE_DIR}/conf/ads_sku_cart_num_top3_by_cate.json" "${WAREHOUSE_DIR}/ads_sku_cart_num_top3_by_cate"
    ;;
    
    "all")
        export_data "${SERVICE_DIR}/conf/ads_new_buyer_stats.json"           "${WAREHOUSE_DIR}/ads_new_buyer_stats"
        export_data "${SERVICE_DIR}/conf/ads_order_by_province.json"         "${WAREHOUSE_DIR}/ads_order_by_province"
        export_data "${SERVICE_DIR}/conf/ads_page_path.json"                 "${WAREHOUSE_DIR}/ads_page_path"
        export_data "${SERVICE_DIR}/conf/ads_repeat_purchase_by_tm.json"     "${WAREHOUSE_DIR}/ads_repeat_purchase_by_tm"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats.json"               "${WAREHOUSE_DIR}/ads_trade_stats"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_cate.json"       "${WAREHOUSE_DIR}/ads_trade_stats_by_cate"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_tm.json"         "${WAREHOUSE_DIR}/ads_trade_stats_by_tm"
        export_data "${SERVICE_DIR}/conf/ads_traffic_stats_by_channel.json"  "${WAREHOUSE_DIR}/ads_traffic_stats_by_channel"
        export_data "${SERVICE_DIR}/conf/ads_user_action.json"               "${WAREHOUSE_DIR}/ads_user_action"
        export_data "${SERVICE_DIR}/conf/ads_user_change.json"               "${WAREHOUSE_DIR}/ads_user_change"
        export_data "${SERVICE_DIR}/conf/ads_user_retention.json"            "${WAREHOUSE_DIR}/ads_user_retention"
        export_data "${SERVICE_DIR}/conf/ads_user_stats.json"                "${WAREHOUSE_DIR}/ads_user_stats"
        export_data "${SERVICE_DIR}/conf/ads_activity_stats.json"            "${WAREHOUSE_DIR}/ads_activity_stats"
        export_data "${SERVICE_DIR}/conf/ads_coupon_stats.json"              "${WAREHOUSE_DIR}/ads_coupon_stats"
        export_data "${SERVICE_DIR}/conf/ads_sku_cart_num_top3_by_cate.json" "${WAREHOUSE_DIR}/ads_sku_cart_num_top3_by_cate"
    ;;
esac
printf "=================================== 运行结束 ===================================\n\n"
exit 0

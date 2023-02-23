#!/usr/bin/env bash


# 定义变量方便修改
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
HDFS_DB_PATH=/warehouse/origin/db                          # 需要同步的数据库数据，在 HDFS 的路径
LOG_FILE="hdfs_ods_db-$(date +%F-%H-%M-%S).log"            # 执行日志
LOG_FILE=hdfs_ods_db.log                                   # 执行日志


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi


function load_data()
{
    sql=""
    for table in $*
    do
        # 判断路径是否存在
        hadoop fs -test -e "${HDFS_DB_PATH}/${table:4}/${do_date}"
        
        # 路径存在方可装载数据
        if [[ $? = 0 ]]; then
            sql=$sql"load data inpath '${HDFS_DB_PATH}/${table:4}/${do_date}' overwrite into table ${HIVE_DATA_BASE}.${table} partition(dt='${do_date}');"
        fi
    done
    
    hive -e "${sql}" >> "$SERVICE_DIR/$LOG_FILE"
}


printf "\n============================== 运行开始 ==============================\n"
case $1 in
    "ods_activity_info_full")
        load_data "ods_activity_info_full"
    ;;

    "ods_activity_rule_full")
        load_data "ods_activity_rule_full"
    ;;

    "ods_base_category1_full")
        load_data "ods_base_category1_full"
    ;;

    "ods_base_category2_full")
        load_data "ods_base_category2_full"
    ;;

    "ods_base_category3_full")
        load_data "ods_base_category3_full"
    ;;

    "ods_base_dic_full")
        load_data "ods_base_dic_full"
    ;;

    "ods_base_province_full")
        load_data "ods_base_province_full"
    ;;

    "ods_base_region_full")
        load_data "ods_base_region_full"
    ;;

    "ods_base_trademark_full")
        load_data "ods_base_trademark_full"
    ;;

    "ods_cart_info_full")
        load_data "ods_cart_info_full"
    ;;

    "ods_coupon_info_full")
        load_data "ods_coupon_info_full"
    ;;

    "ods_sku_attr_value_full")
        load_data "ods_sku_attr_value_full"
    ;;

    "ods_sku_info_full")
        load_data "ods_sku_info_full"
    ;;

    "ods_sku_sale_attr_value_full")
        load_data "ods_sku_sale_attr_value_full"
    ;;

    "ods_spu_info_full")
        load_data "ods_spu_info_full"
    ;;

    "ods_cart_info_inc")
        load_data "ods_cart_info_inc"
    ;;

    "ods_comment_info_inc")
        load_data "ods_comment_info_inc"
    ;;
    "ods_coupon_use_inc")
        load_data "ods_coupon_use_inc"
    ;;

    "ods_favor_info_inc")
        load_data "ods_favor_info_inc"
    ;;

    "ods_order_detail_inc")
        load_data "ods_order_detail_inc"
    ;;

    "ods_order_detail_activity_inc")
        load_data "ods_order_detail_activity_inc"
    ;;

    "ods_order_detail_coupon_inc")
        load_data "ods_order_detail_coupon_inc"
    ;;

    "ods_order_info_inc")
        load_data "ods_order_info_inc"
    ;;

    "ods_order_refund_info_inc")
        load_data "ods_order_refund_info_inc"
    ;;

    "ods_order_status_log_inc")
        load_data "ods_order_status_log_inc"
    ;;

    "ods_payment_info_inc")
        load_data "ods_payment_info_inc"
    ;;

    "ods_refund_payment_inc")
        load_data "ods_refund_payment_inc"
    ;;

    "ods_user_info_inc")
        load_data "ods_user_info_inc"
    ;;

    "all")
        load_data "ods_activity_info_full"        "ods_activity_rule_full"       "ods_base_category1_full" "ods_base_category2_full"   \
                  "ods_base_category3_full"       "ods_base_dic_full"            "ods_base_province_full"  "ods_base_region_full"      \
                  "ods_base_trademark_full"       "ods_cart_info_full"           "ods_coupon_info_full"    "ods_sku_attr_value_full"   \
                  "ods_sku_info_full"             "ods_sku_sale_attr_value_full" "ods_spu_info_full"       "ods_cart_info_inc"         \
                  "ods_comment_info_inc"          "ods_coupon_use_inc"           "ods_favor_info_inc"      "ods_order_detail_inc"      \
                  "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc"  "ods_order_info_inc"      "ods_order_refund_info_inc" \
                  "ods_order_status_log_inc"      "ods_payment_info_inc"         "ods_refund_payment_inc"  "ods_user_info_inc"
    ;;
esac
printf "============================== 运行结束 ==============================\n\n"

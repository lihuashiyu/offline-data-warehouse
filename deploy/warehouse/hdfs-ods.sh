#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  hdfs-ods.sh
#    CreateTime    ：  2023-03-26 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  hdfs-ods.sh 被用于 ==> 将 HDFS 上的数据加载到 Hive 
# =========================================================================================
    
    
# 定义变量方便修改
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
HADOOP_HOME=/opt/apache/hadoop                             # Hadoop 安装路径
HIVE_HOME=/opt/apache/hive                                 # Hive 安装路径
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
WARE_HOUSE_DIR=/warehouse                                  # 原始文件在在 HDFS 上的路径
LOG_FILE="hdfs-ods-$(date +%F).log"                        # 执行日志


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi


# 将用户行为日志 从 HDFS 加载到 Hive
function load_log_data()
{
    echo "****************************** 日志日期为 ${do_date} ******************************"
    sql="load data inpath '${WARE_HOUSE_DIR}/log/${do_date}' into table ${HIVE_DATA_BASE}.ods_log_inc partition(dt='${do_date}');"
    ${HIVE_HOME}/bin/hive -e "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}

# 将业务维度数据 从 HDFS 加载到 Hive
function load_db_data()
{
    for table in $*
    do
        echo "****************************** 业务表：${table:4}，日期：${do_date} ******************************"
        
        # 判断路径是否存在
        ${HADOOP_HOME}/bin/hadoop fs -test -e "${WARE_HOUSE_DIR}/db/${table:4}/${do_date}"
        
        # 路径存在方可装载数据
        if [[ $? = 0 ]]; then
            sql="load data inpath '${WARE_HOUSE_DIR}/db/${table:4}/${do_date}' overwrite into table ${HIVE_DATA_BASE}.${table} partition(dt='${do_date}');"
            ${HIVE_HOME}/bin/hive -e "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
        else
            echo "路径不存在：hdfs:9000//${WARE_HOUSE_DIR}/db/${table:4}/${do_date}"
        fi
    done  
}


printf "\n======================================== 数据加载开始 ========================================\n"
case $1 in
    ods_log_inc)
        load_log_data
    ;;    
        
    ods_activity_info_full)
        load_db_data "ods_activity_info_full"
    ;;
    
    ods_activity_rule_full)
        load_db_data "ods_activity_rule_full"
    ;;
    
    ods_base_category1_full)
        load_db_data "ods_base_category1_full"
    ;;
    
    ods_base_category2_full)
        load_db_data "ods_base_category2_full"
    ;;
    
    ods_base_category3_full)
        load_db_data "ods_base_category3_full"
    ;;
    
    ods_base_dic_full)
        load_db_data "ods_base_dic_full"
    ;;
    
    ods_base_province_full)
        load_db_data "ods_base_province_full"
    ;;
    
    ods_base_region_full)
        load_db_data "ods_base_region_full"
    ;;
    
    ods_base_trademark_full)
        load_db_data "ods_base_trademark_full"
    ;;
    
    ods_cart_info_full)
        load_db_data "ods_cart_info_full"
    ;;
    
    ods_coupon_info_full)
        load_db_data "ods_coupon_info_full"
    ;;
    
    ods_sku_attr_value_full)
        load_db_data "ods_sku_attr_value_full"
    ;;
    
    ods_sku_info_full)
        load_db_data "ods_sku_info_full"
    ;;
    
    ods_sku_sale_attr_value_full)
        load_db_data "ods_sku_sale_attr_value_full"
    ;;
    
    ods_spu_info_full)
        load_db_data "ods_spu_info_full"
    ;;
    
    ods_cart_info_inc)
        load_db_data "ods_cart_info_inc"
    ;;
    
    "ods_comment_info_inc")
        load_db_data "ods_comment_info_inc"
    ;;
    ods_coupon_use_inc)
        load_db_data "ods_coupon_use_inc"
    ;;
    
    ods_favor_info_inc)
        load_db_data "ods_favor_info_inc"
    ;;
    
    ods_order_detail_inc)
        load_db_data "ods_order_detail_inc"
    ;;
    
    "ods_order_detail_activity_inc")
        load_db_data "ods_order_detail_activity_inc"
    ;;
    
    ods_order_detail_coupon_inc)
        load_db_data "ods_order_detail_coupon_inc"
    ;;
    
    ods_order_info_inc)
        load_db_data "ods_order_info_inc"
    ;;
    
    ods_order_refund_info_inc)
        load_db_data "ods_order_refund_info_inc"
    ;;
    
    ods_order_status_log_inc)
        load_db_data "ods_order_status_log_inc"
    ;;
    
    ods_payment_info_inc)
        load_db_data "ods_payment_info_inc"
    ;;
    
    ods_refund_payment_inc)
        load_db_data "ods_refund_payment_inc"
    ;;
    
    ods_user_info_inc)
        load_db_data "ods_user_info_inc"
    ;;
    
    full)
        load_db_data "ods_activity_info_full"  "ods_activity_rule_full"       "ods_base_category1_full"   \
                     "ods_base_category2_full" "ods_base_category3_full"      "ods_base_dic_full"         \
                     "ods_base_province_full"  "ods_base_region_full"         "ods_base_trademark_full"   \
                     "ods_cart_info_full"      "ods_coupon_info_full"         "ods_sku_attr_value_full"   \
                     "ods_sku_info_full"       "ods_sku_sale_attr_value_full" "ods_spu_info_full"
    ;;
    
    inc)
        load_db_data "ods_cart_info_inc"            "ods_comment_info_inc"  "ods_coupon_use_inc"             \
                     "ods_favor_info_inc"           "ods_order_detail_inc"  "ods_order_detail_activity_inc"  \
                     "ods_order_detail_coupon_inc"  "ods_order_info_inc"    "ods_order_refund_info_inc"      \
                     "ods_order_status_log_inc"     "ods_payment_info_inc"  "ods_refund_payment_inc"         \
                     "ods_user_info_inc"
    ;;
    
    db)
        load_db_data "ods_activity_info_full"        "ods_activity_rule_full"       "ods_base_category1_full" "ods_base_category2_full"   \
                     "ods_base_category3_full"       "ods_base_dic_full"            "ods_base_province_full"  "ods_base_region_full"      \
                     "ods_base_trademark_full"       "ods_cart_info_full"           "ods_coupon_info_full"    "ods_sku_attr_value_full"   \
                     "ods_sku_info_full"             "ods_sku_sale_attr_value_full" "ods_spu_info_full"       "ods_cart_info_inc"         \
                     "ods_comment_info_inc"          "ods_coupon_use_inc"           "ods_favor_info_inc"      "ods_order_detail_inc"      \
                     "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc"  "ods_order_info_inc"      "ods_order_refund_info_inc" \
                     "ods_order_status_log_inc"      "ods_payment_info_inc"         "ods_refund_payment_inc"  "ods_user_info_inc"
    ;;
    
    all)
        load_log_data
        load_db_data "ods_activity_info_full"        "ods_activity_rule_full"       "ods_base_category1_full" "ods_base_category2_full"   \
                     "ods_base_category3_full"       "ods_base_dic_full"            "ods_base_province_full"  "ods_base_region_full"      \
                     "ods_base_trademark_full"       "ods_cart_info_full"           "ods_coupon_info_full"    "ods_sku_attr_value_full"   \
                     "ods_sku_info_full"             "ods_sku_sale_attr_value_full" "ods_spu_info_full"       "ods_cart_info_inc"         \
                     "ods_comment_info_inc"          "ods_coupon_use_inc"           "ods_favor_info_inc"      "ods_order_detail_inc"      \
                     "ods_order_detail_activity_inc" "ods_order_detail_coupon_inc"  "ods_order_info_inc"      "ods_order_refund_info_inc" \
                     "ods_order_status_log_inc"      "ods_payment_info_inc"         "ods_refund_payment_inc"  "ods_user_info_inc"
    ;;
    
    *)
        echo "    脚本可传入两个参数，使用方法：/path/$(basename $0) arg1 [arg2] ：                                     "
        echo "        arg1：表名，必填，如下表所示；arg2：日期（yyyy-mm-dd），可选，默认昨天 "
        echo "        +-------------------------------+--------------------------------+ "
        echo "        |            参   数            |            描   述             | "
        echo "        +-------------------------------+--------------------------------+ "
        echo "        | ods_log_inc                   | 行为日志表（增量表）           | "   
        echo "        | ods_activity_info_full        | 活动信息表（全量表）           | "   
        echo "        | ods_activity_rule_full        | 活动规则表（全量表）           | "     
        echo "        | ods_base_category1_full       | 一级品类表（全量表）           | "     
        echo "        | ods_base_category2_full       | 二级分类表（全量表）           | "     
        echo "        | ods_base_category3_full       | 三级分类表（全量表）           | "     
        echo "        | ods_base_dic_full             | 编码字典表（全量表）           | "     
        echo "        | ods_base_province_full        | 省份表（全量表）               | "       
        echo "        | ods_base_region_full          | 地区表（全量表）               | "       
        echo "        | ods_base_trademark_full       | 品牌表（全量表）               | "       
        echo "        | ods_cart_info_full            | 购物车表（全量表）             | "      
        echo "        | ods_coupon_info_full          | 优惠券信息（全量表）           | "     
        echo "        | ods_sku_attr_value_full       | 商品平台属性表（全量表）       | " 
        echo "        | ods_sku_info_full             | 信息表（全量表）               | "   
        echo "        | ods_sku_sale_attr_value_full  | 商品销售属性值（全量表）       | " 
        echo "        | ods_spu_info_full             | SPU 信息表（全量表）           | "
        echo "        | full                          | 所有 ods 全量表                | "
        echo "        | ods_cart_info_inc             | 购物车表（增量表）             | "
        echo "        | ods_comment_info_inc          | 评论表（增量表）               | "
        echo "        | ods_coupon_use_inc            | 优惠券领用表（增量表）         | "
        echo "        | ods_favor_info_inc            | 收藏表（增量表）               | "
        echo "        | ods_order_detail_inc          | 订单明细表（增量表）           | "
        echo "        | ods_order_detail_activity_inc | 订单明细活动关联表（增量表）   | "
        echo "        | ods_order_detail_coupon_inc   | 订单明细优惠券关联表（增量表） | "
        echo "        | ods_order_info_inc            | 订单表（增量表）               | "
        echo "        | ods_order_refund_info_inc     | 退单表（增量表）               | "
        echo "        | ods_order_status_log_inc      | 订单状态流水表（增量表）       | "
        echo "        | ods_payment_info_inc          | 支付表（增量表）               | "
        echo "        | ods_refund_payment_inc        | 退款表（增量表）               | "
        echo "        | ods_user_info_inc             | 用户表（增量表）               | "
        echo "        | inc                           | 所有 ods 增量表                | "
        echo "        | db                            | 所有 ods 业务数据表            | "
        echo "        | all                           | 全部 ods 业务数据和行为日志表  | "   
        echo "        +-------------------------------+--------------------------------+ "
    ;;
esac
printf "======================================== 运行结束 ========================================\n\n"
exit 0

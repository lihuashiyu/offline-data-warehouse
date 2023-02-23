#!/usr/bin/env bash


APP=gmall
WARE_HOUSE_DIR=/user/warehouse                            # 仓库在 HDFS 上的路径


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi


ods_order_info="load data inpath '${WARE_HOUSE_DIR}/${APP}/db/order_info/${do_date}' overwrite into table ${APP}.ods_order_info partition(dt='${do_date}');"

ods_order_detail="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/order_detail/${do_date}' overwrite into table ${APP}.ods_order_detail partition(dt='${do_date}');"

ods_sku_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/sku_info/${do_date}' overwrite into table ${APP}.ods_sku_info partition(dt='${do_date}');"

ods_user_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/user_info/${do_date}' overwrite into table ${APP}.ods_user_info partition(dt='${do_date}');"

ods_payment_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/payment_info/${do_date}' overwrite into table ${APP}.ods_payment_info partition(dt='${do_date}');"

ods_base_category1="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_category1/${do_date}' overwrite into table ${APP}.ods_base_category1 partition(dt='${do_date}');"

ods_base_category2="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_category2/${do_date}' overwrite into table ${APP}.ods_base_category2 partition(dt='${do_date}');"

ods_base_category3="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_category3/${do_date}' overwrite into table ${APP}.ods_base_category3 partition(dt='${do_date}'); "

ods_base_trademark="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_trademark/${do_date}' overwrite into table ${APP}.ods_base_trademark partition(dt='${do_date}'); "

ods_activity_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/activity_info/${do_date}' overwrite into table ${APP}.ods_activity_info partition(dt='${do_date}'); "

ods_cart_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/cart_info/${do_date}' overwrite into table ${APP}.ods_cart_info partition(dt='${do_date}');"

ods_comment_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/comment_info/${do_date}' overwrite into table ${APP}.ods_comment_info partition(dt='${do_date}'); "

ods_coupon_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/coupon_info/${do_date}' overwrite into table ${APP}.ods_coupon_info partition(dt='${do_date}'); "

ods_coupon_use="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/coupon_use/${do_date}' overwrite into table ${APP}.ods_coupon_use partition(dt='${do_date}'); "

ods_favor_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/favor_info/${do_date}' overwrite into table ${APP}.ods_favor_info partition(dt='${do_date}'); "

ods_order_refund_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/order_refund_info/${do_date}' overwrite into table ${APP}.ods_order_refund_info partition(dt='${do_date}');"

ods_order_status_log="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/order_status_log/${do_date}' overwrite into table ${APP}.ods_order_status_log partition(dt='${do_date}'); "

ods_spu_info="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/spu_info/${do_date}' overwrite into table ${APP}.ods_spu_info partition(dt='${do_date}');"

ods_activity_rule="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/activity_rule/${do_date}' overwrite into table ${APP}.ods_activity_rule partition(dt='${do_date}');"

ods_base_dic="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_dic/${do_date}' overwrite into table ${APP}.ods_base_dic partition(dt='${do_date}');"

ods_order_detail_activity="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/order_detail_activity/${do_date}' overwrite into table ${APP}.ods_order_detail_activity partition(dt='${do_date}');"

ods_order_detail_coupon="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/order_detail_coupon/${do_date}' overwrite into table ${APP}.ods_order_detail_coupon partition(dt='${do_date}'); "

ods_refund_payment="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/refund_payment/${do_date}' overwrite into table ${APP}.ods_refund_payment partition(dt='${do_date}'); "

ods_sku_attr_value="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/sku_attr_value/${do_date}' overwrite into table ${APP}.ods_sku_attr_value partition(dt='${do_date}');"

ods_sku_sale_attr_value=" load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/sku_sale_attr_value/${do_date}' overwrite into table ${APP}.ods_sku_sale_attr_value partition(dt='${do_date}'); "

ods_base_province="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_province/${do_date}' overwrite into table ${APP}.ods_base_province;"

ods_base_region="load data inpath '${WARE_HOUSE_DIR}/'${APP}/db/base_region/${do_date}' overwrite into table ${APP}.ods_base_region;"

case $1 in
    "ods_order_info")
        hive -e "${ods_order_info}"
    ;;
    "ods_order_detail")
        hive -e "${ods_order_detail}"
    ;;
    "ods_sku_info")
        hive -e "${ods_sku_info}"
    ;;
    "ods_user_info")
        hive -e "${ods_user_info}"
    ;;
    "ods_payment_info")
        hive -e "${ods_payment_info}"
    ;;
    "ods_base_category1")
        hive -e "${ods_base_category1}"
    ;;
    "ods_base_category2")
        hive -e "${ods_base_category2}"
    ;;
    "ods_base_category3")
        hive -e "${ods_base_category3}"
    ;;
    "ods_base_trademark")
        hive -e "${ods_base_trademark}"
    ;;
    "ods_activity_info")
        hive -e "${ods_activity_info}"
    ;;
    "ods_cart_info")
        hive -e "${ods_cart_info}"
    ;;
    "ods_comment_info")
        hive -e "${ods_comment_info}"
    ;;
    "ods_coupon_info")
        hive -e "${ods_coupon_info}"
    ;;
    "ods_coupon_use")
        hive -e "${ods_coupon_use}"
    ;;
    "ods_favor_info")
        hive -e "${ods_favor_info}"
    ;;
    "ods_order_refund_info")
        hive -e "${ods_order_refund_info}"
    ;;
    "ods_order_status_log")
        hive -e "${ods_order_status_log}"
    ;;
    "ods_spu_info")
        hive -e "${ods_spu_info}"
    ;;
    "ods_activity_rule")
        hive -e "${ods_activity_rule}"
    ;;
    "ods_base_dic")
        hive -e "${ods_base_dic}"
    ;;
    "ods_order_detail_activity")
        hive -e "${ods_order_detail_activity}"
    ;;
    "ods_order_detail_coupon")
        hive -e "${ods_order_detail_coupon}"
    ;;
    "ods_refund_payment")
        hive -e "${ods_refund_payment}"
    ;;
    "ods_sku_attr_value")
        hive -e "${ods_sku_attr_value}"
    ;;
    "ods_sku_sale_attr_value")
        hive -e "${ods_sku_sale_attr_value}"
    ;;
    "ods_base_province")
        hive -e "${ods_base_province}"
    ;;
    "ods_base_region")
        hive -e "${ods_base_region}"
    ;;
    "all"){
    hive -e "${ods_order_info}            ${ods_order_detail}        ${ods_sku_info}       ${ods_user_info} 
             ${ods_payment_info}          ${ods_base_category1}      ${ods_base_category2} ${ods_base_category3} 
             ${ods_base_trademark}        ${ods_activity_info}       ${ods_cart_info}      ${ods_comment_info} 
             ${ods_coupon_info}           ${ods_coupon_use}          ${ods_favor_info}     ${ods_order_refund_info} 
             ${ods_order_status_log}      ${ods_spu_info}            ${ods_activity_rule}  ${ods_base_dic} 
             ${ods_order_detail_activity} ${ods_order_detail_coupon} ${ods_refund_payment} ${ods_sku_attr_value} 
             ${ods_sku_sale_attr_value}   ${ods_base_province}       ${ods_base_region}"
    };;
esac



#!/usr/bin/env bash


APP=gmall


if [ -n "$2" ] ;then
    do_date=$2
else 
    echo "请传入日期参数"
    exit
fi 


dwd_order_info="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_order_info partition(dt)
    select oi.id,
           oi.order_status,
           oi.user_id,
           oi.province_id,
           oi.payment_way,
           oi.delivery_address,
           oi.out_trade_no,
           oi.tracking_no,
           oi.create_time,
           times.ts['1002'] payment_time,
           times.ts['1003'] cancel_time,
           times.ts['1004'] finish_time,
           times.ts['1005'] refund_time,
           times.ts['1006'] refund_finish_time,
           oi.expire_time,
           feight_fee,
           feight_fee_reduce,
           activity_reduce_amount,
           coupon_reduce_amount,
           original_amount,
           final_amount,
           case
               when times.ts['1003'] is not null then date_format(times.ts['1003'],'yyyy-MM-dd')
               when times.ts['1004'] is not null and date_add(date_format(times.ts['1004'],'yyyy-MM-dd'),7)<='$do_date' and times.ts['1005'] is null then date_add(date_format(times.ts['1004'],'yyyy-MM-dd'),7)
               when times.ts['1006'] is not null then date_format(times.ts['1006'],'yyyy-MM-dd')
               when oi.expire_time is not null   then date_format(oi.expire_time,'yyyy-MM-dd')
               else '9999-99-99'
           end
    from
    (
        select *
        from ${APP}.ods_order_info
        where dt='${do_date}'
    ) oi left join
    (
        select
            order_id,
            str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') ts
        from ${APP}.ods_order_status_log
        where dt='${do_date}'
        group by order_id
    ) times on oi.id=times.order_id;"

dwd_order_detail="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_order_detail partition(dt)
    select od.id,
           od.order_id,
           oi.user_id,
           od.sku_id,
           oi.province_id,
           oda.activity_id,
           oda.activity_rule_id,
           odc.coupon_id,
           od.create_time,
           od.source_type,
           od.source_id,
           od.sku_num,
           od.order_price*od.sku_num,
           od.split_activity_amount,
           od.split_coupon_amount,
           od.split_final_amount,
           date_format(create_time,'yyyy-MM-dd')
    from
    (
        select *
        from ${APP}.ods_order_detail
        where dt='${do_date}'
    ) od left join
    (
        select id,
               user_id,
               province_id
        from ${APP}.ods_order_info
        where dt='${do_date}'
    ) oi on od.order_id=oi.id left join
    (
        select order_detail_id,
               activity_id,
               activity_rule_id
        from ${APP}.ods_order_detail_activity
        where dt='${do_date}'
    ) oda on od.id=oda.order_detail_id left join
    (
        select order_detail_id,
               coupon_id
        from ${APP}.ods_order_detail_coupon
        where dt='${do_date}'
    ) odc on od.id=odc.order_detail_id;"

dwd_payment_info="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_payment_info partition(dt)
    select pi.id,
           pi.order_id,
           pi.user_id,
           oi.province_id,
           pi.trade_no,
           pi.out_trade_no,
           pi.payment_type,
           pi.payment_amount,
           pi.payment_status,
           pi.create_time,
           pi.callback_time,
           nvl(date_format(pi.callback_time,'yyyy-MM-dd'),'9999-99-99')
    from
    (
        select * from ${APP}.ods_payment_info where dt='${do_date}'
    ) pi left join
    (
        select id,province_id from ${APP}.ods_order_info where dt='${do_date}'
    ) oi on pi.order_id=oi.id;"

dwd_cart_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_cart_info partition(dt='${do_date}')
    select id,
           user_id,
           sku_id,
           source_type,
           source_id,
           cart_price,
           is_ordered,
           create_time,
           operate_time,
           order_time,
           sku_num
    from ${APP}.ods_cart_info
    where dt='${do_date}';"

dwd_comment_info="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_comment_info partition(dt)
    select id,
           user_id,
           sku_id,
           spu_id,
           order_id,
           appraise,
           create_time,
           date_format(create_time,'yyyy-MM-dd')
    from ${APP}.ods_comment_info
    where dt='${do_date}';"

dwd_favor_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_favor_info partition(dt='${do_date}')
    select id,
           user_id,
           sku_id,
           spu_id,
           is_cancel,
           create_time,
           cancel_time
    from ${APP}.ods_favor_info
    where dt='${do_date}';"

dwd_coupon_use="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_coupon_use partition(dt)
    select id,
           coupon_id,
           user_id,
           order_id,
           coupon_status,
           get_time,
           using_time,
           used_time,
           expire_time,
           coalesce(date_format(used_time,'yyyy-MM-dd'),date_format(expire_time,'yyyy-MM-dd'), '9999-99-99')
    from ${APP}.ods_coupon_use
    where dt='${do_date}';"

dwd_order_refund_info="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_order_refund_info partition(dt)
    select ri.id,
           ri.user_id,
           ri.order_id,
           ri.sku_id,
           oi.province_id,
           ri.refund_type,
           ri.refund_num,
           ri.refund_amount,
           ri.refund_reason_type,
           ri.create_time,
           date_format(ri.create_time,'yyyy-MM-dd')
    from
    (
        select * from ${APP}.ods_order_refund_info where dt='${do_date}'
    ) ri left join
    (
        select id,province_id from ${APP}.ods_order_info where dt='${do_date}'
    ) oi on ri.order_id=oi.id;"

dwd_refund_payment="
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_refund_payment partition(dt)
    select rp.id,
           user_id,
           order_id,
           sku_id,
           province_id,
           trade_no,
           out_trade_no,
           payment_type,
           refund_amount,
           refund_status,
           create_time,
           callback_time,
           nvl(date_format(callback_time,'yyyy-MM-dd'),'9999-99-99')
    from
    (
        select id,
               out_trade_no,
               order_id,
               sku_id,
               payment_type,
               trade_no,
               refund_amount,
               refund_status,
               create_time,
               callback_time
        from ${APP}.ods_refund_payment
        where dt='${do_date}'
    ) rp left join
    (
        select id,
               user_id,
               province_id
        from ${APP}.ods_order_info
        where dt='${do_date}'
    ) oi on rp.order_id=oi.id;"


case $1 in
    dwd_order_info )
        hive -e "${dwd_order_info}"
    ;;
    dwd_order_detail )
        hive -e "${dwd_order_detail}"
    ;;
    dwd_payment_info )
        hive -e "${dwd_payment_info}"
    ;;
    dwd_cart_info )
        hive -e "${dwd_cart_info}"
    ;;
    dwd_comment_info )
        hive -e "${dwd_comment_info}"
    ;;
    dwd_favor_info )
        hive -e "${dwd_favor_info}"
    ;;
    dwd_coupon_use )
        hive -e "${dwd_coupon_use}"
    ;;
    dwd_order_refund_info )
        hive -e "${dwd_order_refund_info}"
    ;;
    dwd_refund_payment )
        hive -e "${dwd_refund_payment}"
    ;;
    all )
        hive -e "${dwd_order_info} ${dwd_order_detail} ${dwd_payment_info}      ${dwd_cart_info}     ${dwd_comment_info} 
                 ${dwd_favor_info} ${dwd_coupon_use}   ${dwd_order_refund_info} ${dwd_refund_payment}"
    ;;
esac


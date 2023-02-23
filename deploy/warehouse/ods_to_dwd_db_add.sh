#!/usr/bin/env bash


APP=gmall


if [ -n "$2" ] ;then
    do_date=$2
else 
    echo "请传入日期参数"
    exit
fi 


# 假设某累积型快照事实表，某天所有的业务记录全部完成，则会导致9999-99-99分区的数据未被覆盖，
# 从而导致数据重复，该函数根据9999-99-99分区的数据的末次修改时间判断其是否被覆盖了，如果未被覆盖，就手动清理
clear_data()
{
    current_date=$(date +%F)
    current_date_timestamp=$(date -d "$current_date" +%s)

    last_modified_date=$(hadoop fs -ls /warehouse/gmall/dwd/$1 | grep '9999-99-99' | awk '{print $6}')
    last_modified_date_timestamp=$(date -d "$last_modified_date" +%s)

    if [[ $last_modified_date_timestamp -lt $current_date_timestamp ]]; then
        echo "clear table $1 partition(dt=9999-99-99)"
        hadoop fs -rm -r -f /warehouse/gmall/dwd/$1/dt=9999-99-99/*
    fi
}


dwd_order_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${APP}.dwd_order_info partition(dt)
    select nvl(new.id,old.id),
           nvl(new.order_status,old.order_status),
           nvl(new.user_id,old.user_id),
           nvl(new.province_id,old.province_id),
           nvl(new.payment_way,old.payment_way),
           nvl(new.delivery_address,old.delivery_address),
           nvl(new.out_trade_no,old.out_trade_no),
           nvl(new.tracking_no,old.tracking_no),
           nvl(new.create_time,old.create_time),
           nvl(new.payment_time,old.payment_time),
           nvl(new.cancel_time,old.cancel_time),
           nvl(new.finish_time,old.finish_time),
           nvl(new.refund_time,old.refund_time),
           nvl(new.refund_finish_time,old.refund_finish_time),
           nvl(new.expire_time,old.expire_time),
           nvl(new.feight_fee,old.feight_fee),
           nvl(new.feight_fee_reduce,old.feight_fee_reduce),
           nvl(new.activity_reduce_amount,old.activity_reduce_amount),
           nvl(new.coupon_reduce_amount,old.coupon_reduce_amount),
           nvl(new.original_amount,old.original_amount),
           nvl(new.final_amount,old.final_amount),
           case
               when new.cancel_time is not null then date_format(new.cancel_time,'yyyy-MM-dd')
               when new.finish_time is not null and date_add(date_format(new.finish_time,'yyyy-MM-dd'),7)='$do_date' and new.refund_time is null then '$do_date'
               when new.refund_finish_time is not null then date_format(new.refund_finish_time,'yyyy-MM-dd')
               when new.expire_time is not null then date_format(new.expire_time,'yyyy-MM-dd')
               else '9999-99-99'
           end
    from
    (
        select id,
               order_status,
               user_id,
               province_id,
               payment_way,
               delivery_address,
               out_trade_no,
               tracking_no,
               create_time,
               payment_time,
               cancel_time,
               finish_time,
               refund_time,
               refund_finish_time,
               expire_time,
               feight_fee,
               feight_fee_reduce,
               activity_reduce_amount,
               coupon_reduce_amount,
               original_amount,
               final_amount
        from ${APP}.dwd_order_info
        where dt='9999-99-99'
    ) old full outer join
    (
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
               final_amount
        from
        (
            select *
            from ${APP}.ods_order_info
            where dt='${do_date}'
        ) oi left join
        (
            select order_id,
                   str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') ts
            from ${APP}.ods_order_status_log
            where dt='${do_date}'
            group by order_id
        ) times on oi.id=times.order_id
    ) new on old.id=new.id;"

dwd_order_detail="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_order_detail partition(dt='${do_date}')
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
           od.split_final_amount
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
    ) oi on od.order_id=oi.id
    left join
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
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${APP}.dwd_payment_info partition(dt)
    select nvl(new.id,old.id),
           nvl(new.order_id,old.order_id),
           nvl(new.user_id,old.user_id),
           nvl(new.province_id,old.province_id),
           nvl(new.trade_no,old.trade_no),
           nvl(new.out_trade_no,old.out_trade_no),
           nvl(new.payment_type,old.payment_type),
           nvl(new.payment_amount,old.payment_amount),
           nvl(new.payment_status,old.payment_status),
           nvl(new.create_time,old.create_time),
           nvl(new.callback_time,old.callback_time),
           nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
    from
    (
        select id,
               order_id,
               user_id,
               province_id,
               trade_no,
               out_trade_no,
               payment_type,
               payment_amount,
               payment_status,
               create_time,
               callback_time
        from ${APP}.dwd_payment_info
        where dt = '9999-99-99'
    ) old full outer join
    (
        select pi.id,
               pi.out_trade_no,
               pi.order_id,
               pi.user_id,
               oi.province_id,
               pi.payment_type,
               pi.trade_no,
               pi.payment_amount,
               pi.payment_status,
               pi.create_time,
               pi.callback_time
        from
        (
            select * from ${APP}.ods_payment_info where dt='${do_date}'
        ) pi left join
        (
            select id,province_id from ${APP}.ods_order_info where dt='${do_date}'
        ) oi on pi.order_id=oi.id
    ) new on old.id=new.id;"

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
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_comment_info partition(dt='${do_date}')
    select id,
           user_id,
           sku_id,
           spu_id,
           order_id,
           appraise,
           create_time
    from ${APP}.ods_comment_info where dt='${do_date}';"

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
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${APP}.dwd_coupon_use partition(dt)
    select nvl(new.id,old.id),
           nvl(new.coupon_id,old.coupon_id),
           nvl(new.user_id,old.user_id),
           nvl(new.order_id,old.order_id),
           nvl(new.coupon_status,old.coupon_status),
           nvl(new.get_time,old.get_time),
           nvl(new.using_time,old.using_time),
           nvl(new.used_time,old.used_time),
           nvl(new.expire_time,old.expire_time),
           coalesce(date_format(nvl(new.used_time,old.used_time),'yyyy-MM-dd'),date_format(nvl(new.expire_time,old.expire_time),'yyyy-MM-dd'),'9999-99-99')
    from
    (
        select id,
               coupon_id,
               user_id,
               order_id,
               coupon_status,
               get_time,
               using_time,
               used_time,
               expire_time
        from ${APP}.dwd_coupon_use
        where dt='9999-99-99'
    ) old full outer join
    (
        select id,
               coupon_id,
               user_id,
               order_id,
               coupon_status,
               get_time,
               using_time,
               used_time,
               expire_time
        from ${APP}.ods_coupon_use
        where dt='${do_date}'
    ) new on old.id=new.id;"

dwd_order_refund_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dwd_order_refund_info partition(dt='${do_date}')
    select ri.id,
           ri.user_id,
           ri.order_id,
           ri.sku_id,
           oi.province_id,
           ri.refund_type,
           ri.refund_num,
           ri.refund_amount,
           ri.refund_reason_type,
           ri.create_time
    from
    (
        select * from ${APP}.ods_order_refund_info where dt='${do_date}'
    ) ri left join
    (
        select id,province_id from ${APP}.ods_order_info where dt='${do_date}'
    ) oi on ri.order_id=oi.id;"

dwd_refund_payment="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${APP}.dwd_refund_payment partition(dt)
    select nvl(new.id,old.id),
           nvl(new.user_id,old.user_id),
           nvl(new.order_id,old.order_id),
           nvl(new.sku_id,old.sku_id),
           nvl(new.province_id,old.province_id),
           nvl(new.trade_no,old.trade_no),
           nvl(new.out_trade_no,old.out_trade_no),
           nvl(new.payment_type,old.payment_type),
           nvl(new.refund_amount,old.refund_amount),
           nvl(new.refund_status,old.refund_status),
           nvl(new.create_time,old.create_time),
           nvl(new.callback_time,old.callback_time),
           nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
    from
    (
        select id,
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
               callback_time
        from ${APP}.dwd_refund_payment
        where dt='9999-99-99'
    ) old full outer join
    (
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
               callback_time
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
        ) oi on rp.order_id=oi.id
    ) new on old.id=new.id;"


case $1 in
    dwd_order_info )
        hive -e "${dwd_order_info}"
        clear_data dwd_order_info
    ;;
    dwd_order_detail )
        hive -e "${dwd_order_detail}"
    ;;
    dwd_payment_info )
        hive -e "${dwd_payment_info}"
        clear_data dwd_payment_info
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
        clear_data dwd_coupon_use
    ;;
    dwd_order_refund_info )
        hive -e "${dwd_order_refund_info}"
    ;;
    dwd_refund_payment )
        hive -e "${dwd_refund_payment}"
        clear_data dwd_refund_payment
    ;;
    all )
        hive -e "${dwd_order_info}  ${dwd_order_detail}       ${dwd_payment_info}
                 ${dwd_cart_info}   ${dwd_comment_info}       ${dwd_favor_info}   
                 ${dwd_coupon_use}  ${dwd_order_refund_info}  ${dwd_refund_payment}"
                    
        clear_data dwd_order_info
        clear_data dwd_payment_info
        clear_data dwd_coupon_use
        clear_data dwd_refund_payment
    ;;
esac


#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  dwd-dws-init.sh
#    CreateTime    ：  2023-03-26 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  dwd-dws-init.sh 被用于 ==> 将 DWD 层数据加载到 DWS，仅初始化执行
# =========================================================================================
    
    
# 定义变量方便修改
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
HIVE_HOME=/opt/apache/hive                                 # Hive 的安装位置
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
LOG_FILE="dwd-dws-init-$(date +%F).log"                    # 执行日志


if [ -n "$2" ]; then
    do_date=$2
else
    echo "请传入日期参数"
    exit
fi


dws_trade_user_sku_order_1d="
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_sku_order_1d partition (dt)
    select order_detail.user_id,
           sku.id,
           sku.sku_name,
           sku.category1_id,
           sku.category1_name,
           sku.category2_id,
           sku.category2_name,
           sku.category3_id,
           sku.category3_name,
           sku.tm_id,
           sku.tm_name,
           order_detail.order_count_1d,
           order_detail.order_num_1d,
           order_detail.order_original_amount_1d,
           order_detail.activity_reduce_amount_1d,
           order_detail.coupon_reduce_amount_1d,
           order_detail.order_total_amount_1d,
           order_detail.dt
    from 
    (
        select dt,
               user_id,
               sku_id,
               count(*)                             as order_count_1d,
               sum(sku_num)                         as order_num_1d,
               sum(split_original_amount)           as order_original_amount_1d,
               sum(nvl(split_activity_amount, 0.0)) as activity_reduce_amount_1d,
               sum(nvl(split_coupon_amount,   0.0)) as coupon_reduce_amount_1d,
               sum(split_total_amount)              as order_total_amount_1d
        from ${HIVE_DATA_BASE}.dwd_trade_order_detail_inc
        group by dt, user_id, sku_id
    ) as order_detail left join 
    (
        select id,
               sku_name,
               category1_id,
               category1_name,
               category2_id,
               category2_name,
               category3_id,
               category3_name,
               tm_id,
               tm_name
       from ${HIVE_DATA_BASE}.dim_sku_full
       where dt = '${do_date}'
    ) as sku on order_detail.sku_id = sku.id;
    set hive.exec.dynamic.partition.mode=strict;
"

dws_trade_user_sku_order_refund_1d="
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_1d partition (dt)
    select order_refund.user_id,
           order_refund.sku_id,
           sku.sku_name,
           sku.category1_id,
           sku.category1_name,
           sku.category2_id,
           sku.category2_name,
           sku.category3_id,
           sku.category3_name,
           sku.tm_id,
           sku.tm_name,
           order_refund.order_refund_count,
           order_refund.order_refund_num,
           order_refund.order_refund_amount,
           order_refund.dt
    from 
    (
        select dt,
               user_id,
               sku_id,
               count(*)           as order_refund_count,
               sum(refund_num)    as order_refund_num,
               sum(refund_amount) as order_refund_amount
        from ${HIVE_DATA_BASE}.dwd_trade_order_refund_inc
        group by dt, user_id, sku_id
    ) as order_refund left join 
    (
        select id,
               sku_name,
               category1_id,
               category1_name,
               category2_id,
               category2_name,
               category3_id,
               category3_name,
               tm_id,
               tm_name
        from ${HIVE_DATA_BASE}.dim_sku_full
        where dt = '${do_date}'
    ) as sku on order_refund.sku_id = sku.id;
    set hive.exec.dynamic.partition.mode=strict;
"

dws_trade_user_order_1d="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_order_1d partition (dt)
    select user_id,
           count(distinct (order_id))         as order_count_1d,
           sum(sku_num)                       as order_num_1d,
           sum(split_original_amount)         as order_original_amount_1d,
           sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,
           sum(nvl(split_coupon_amount,   0)) as coupon_reduce_amount_1d,
           sum(split_total_amount)            as order_total_amount_1d,
           dt
    from ${HIVE_DATA_BASE}.dwd_trade_order_detail_inc
    group by user_id, dt;
"

dws_trade_user_cart_add_1d="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_cart_add_1d partition (dt)
    select user_id, 
           count(*)      as cart_add_count_1d, 
           sum(sku_num)  as cart_add_num_1d,  
           dt
    from ${HIVE_DATA_BASE}.dwd_trade_cart_add_inc
    group by user_id, dt;
"

dws_trade_user_payment_1d="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_payment_1d partition (dt)
    select user_id, 
           count(distinct (order_id)) as payment_count_1d, 
           sum(sku_num)               as payment_num_1d, 
           sum(split_payment_amount)  as payment_amount_1d, 
           dt
    from ${HIVE_DATA_BASE}.dwd_trade_pay_detail_suc_inc
    group by user_id, dt;
"

dws_trade_user_order_refund_1d="
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_order_refund_1d partition (dt)
    select user_id,
           count(*)           as order_refund_count,
           sum(refund_num)    as order_refund_num,
           sum(refund_amount) as order_refund_amount,
           dt
    from ${HIVE_DATA_BASE}.dwd_trade_order_refund_inc
    group by user_id, dt;
"

dws_trade_province_order_1d="
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_province_order_1d partition (dt)
    select province.id                             as province_id,
           province.province_name,
           province.area_code,
           province.iso_code,
           province.iso_3166_2,
           order_detail.order_count_1d,
           order_detail.order_original_amount_1d,
           order_detail.activity_reduce_amount_1d,
           order_detail.coupon_reduce_amount_1d,
           order_detail.order_total_amount_1d,
           order_detail.dt
    from 
    (
        select province_id,
               count(distinct (order_id))         as order_count_1d,
               sum(split_original_amount)         as order_original_amount_1d,
               sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,
               sum(nvl(split_coupon_amount, 0))   as coupon_reduce_amount_1d,
               sum(split_total_amount)            as order_total_amount_1d,
               dt
        from ${HIVE_DATA_BASE}.dwd_trade_order_detail_inc
        group by province_id, dt
    ) as order_detail left join 
    (
        select id, 
               province_name, 
               area_code, 
               iso_code, 
               iso_3166_2
        from ${HIVE_DATA_BASE}.dim_province_full
        where dt = '${do_date}'
    ) as province on order_detail.province_id = province.id;
    set hive.exec.dynamic.partition.mode=strict;
"

dws_traffic_session_page_view_1d="
    insert overwrite table ${HIVE_DATA_BASE}.dws_traffic_session_page_view_1d partition (dt = '${do_date}')
    select session_id,
           mid_id,
           brand,
           model,
           operate_system,
           version_code,
           channel,
           sum(during_time) as during_time_1d,
           count(*)         as page_count_1d
    from ${HIVE_DATA_BASE}.dwd_traffic_page_view_inc
    where dt = '${do_date}'
    group by session_id, mid_id, brand, model, operate_system, version_code, channel;
"

dws_traffic_page_visitor_page_view_1d="
    insert overwrite table ${HIVE_DATA_BASE}.dws_traffic_page_visitor_page_view_1d partition (dt = '${do_date}')
    select mid_id, 
           brand, 
           model, 
           operate_system, 
           page_id, 
           sum(during_time) as during_time_1d, 
           count(*)         as view_count_1d
    from ${HIVE_DATA_BASE}.dwd_traffic_page_view_inc
    where dt = '${do_date}'
    group by mid_id, brand, model, operate_system, page_id;
"
dws_trade_user_sku_order_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_sku_order_nd partition (dt = '${do_date}')
    select user_id,
           sku_id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           tm_id,
           tm_name,
           sum(if(dt >= date_add('${do_date}', -6), order_count_1d,            0)) as order_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_num_1d,              0)) as order_num_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
           sum(order_count_1d)                                                     as order_count_30d,
           sum(order_num_1d)                                                       as order_num_30d,
           sum(order_original_amount_1d)                                           as order_original_amount_30d,
           sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,
           sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
           sum(order_total_amount_1d)                                              as order_total_amount_30d    
    from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,
             category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;
"

dws_trade_user_sku_order_refund_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_nd partition (dt = '${do_date}')
    select user_id,
           sku_id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           tm_id,
           tm_name,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,
           sum(order_refund_count_1d)                                           as order_refund_count_30d,
           sum(order_refund_num_1d)                                             as order_refund_num_30d,
           sum(order_refund_amount_1d)                                          as order_refund_amount_30d
    from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,
             category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;
"

dws_trade_user_order_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_order_nd partition (dt = '${do_date}')
        select user_id,
               sum(if(dt >= date_add('${do_date}', -6), order_count_1d,            0)) as order_count_7d,
               sum(if(dt >= date_add('${do_date}', -6), order_num_1d,              0)) as order_num_7d,
               sum(if(dt >= date_add('${do_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
               sum(if(dt >= date_add('${do_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
               sum(if(dt >= date_add('${do_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
               sum(if(dt >= date_add('${do_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
               sum(order_count_1d)                                                     as order_count_30d,
               sum(order_num_1d)                                                       as order_num_30d,
               sum(order_original_amount_1d)                                           as order_original_amount_30d,
               sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,
               sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
               sum(order_total_amount_1d)                                              as order_total_amount_30d
    from ${HIVE_DATA_BASE}.dws_trade_user_order_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id;
"

dws_trade_user_cart_add_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_cart_add_nd partition (dt = '${do_date}')
    select user_id,
           sum(if(dt >= date_add('${do_date}', -6), cart_add_count_1d, 0)) as cart_add_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), cart_add_num_1d,   0)) as cart_add_num_7d,
           sum(cart_add_count_1d)                                          as cart_add_count_30d,
           sum(cart_add_num_1d)                                            as cart_add_num_30d
    from ${HIVE_DATA_BASE}.dws_trade_user_cart_add_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id;
"

dws_trade_user_payment_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_payment_nd partition (dt = '${do_date}')
    select user_id,
           sum(if(dt >= date_add('${do_date}', -6), payment_count_1d,  0)) as payment_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), payment_num_1d,    0)) as payment_num_7d,
           sum(if(dt >= date_add('${do_date}', -6), payment_amount_1d, 0)) as payment_amount_7d,
           sum(payment_count_1d)                                           as payment_count_30d,
           sum(payment_num_1d)                                             as payment_num_30d,
           sum(payment_amount_1d)                                          as payment_amount_30d
    from ${HIVE_DATA_BASE}.dws_trade_user_payment_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id;
"

dws_trade_user_order_refund_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_order_refund_nd partition (dt = '${do_date}')
    select user_id,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,
           sum(order_refund_count_1d)                                           as order_refund_count_30d,
           sum(order_refund_num_1d)                                             as order_refund_num_30d,
           sum(order_refund_amount_1d)                                          as order_refund_amount_30d
    from ${HIVE_DATA_BASE}.dws_trade_user_order_refund_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by user_id;
"

dws_trade_province_order_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_province_order_nd partition (dt = '${do_date}')
    select province_id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2,
           sum(if(dt >= date_add('${do_date}', -6), order_count_1d,            0)) as order_count_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
           sum(if(dt >= date_add('${do_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
           sum(order_count_1d)                                                     as order_count_30d,
           sum(order_original_amount_1d)                                           as order_original_amount_30d,
           sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30,
           sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
           sum(order_total_amount_1d)                                              as order_total_amount_30d
    from ${HIVE_DATA_BASE}.dws_trade_province_order_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by province_id, province_name, area_code, iso_code, iso_3166_2;
"

dws_trade_coupon_order_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_coupon_order_nd partition (dt = '${do_date}')
    select coupon.id,
           coupon.coupon_name,
           coupon.coupon_type_code,
           coupon.coupon_type_name,
           coupon.benefit_rule,
           coupon.start_date,
           sum(order_detail.split_original_amount) as original_amount_30d,
           sum(order_detail.split_coupon_amount)   as coupon_reduce_amount_30d
    from 
    (
        select id,
               coupon_name,
               coupon_type_code,
               coupon_type_name,
               benefit_rule,
               date_format(start_time, 'yyyy-MM-dd') as start_date
        from ${HIVE_DATA_BASE}.dim_coupon_full
        where dt = '${do_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('${do_date}', -29)
    ) as coupon left join 
    (
        select coupon_id, 
               order_id, 
               split_original_amount, 
               split_coupon_amount
        from ${HIVE_DATA_BASE}.dwd_trade_order_detail_inc
        where dt >= date_add('${do_date}', -29) and dt <= '${do_date}' and coupon_id is not null
    ) as order_detail 
        on coupon.id = order_detail.coupon_id
    group by coupon.id,               coupon.coupon_name,  coupon.coupon_type_code,
             coupon.coupon_type_name, coupon.benefit_rule, coupon.start_date;
"

dws_trade_activity_order_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_activity_order_nd partition (dt = '${do_date}')
    select activity.activity_id,
           activity.activity_name,
           activity.activity_type_code,
           activity.activity_type_name,
           date_format(activity.start_time, 'yyyy-MM-dd') as start_date,
           sum(order_detail.split_original_amount)        as original_amount_30d,
           sum(order_detail.split_activity_amount)        as activity_reduce_amount_30d
    from 
    (
        select activity_id, 
               activity_name, 
               activity_type_code, 
               activity_type_name, 
               start_time
        from ${HIVE_DATA_BASE}.dim_activity_full
        where dt = '${do_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('${do_date}', -29)
        group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
    ) as activity left join 
    (
        select activity_id, 
               order_id, 
               split_original_amount, 
               split_activity_amount
        from ${HIVE_DATA_BASE}.dwd_trade_order_detail_inc
        where dt >= date_add('${do_date}', -29) and dt <= '${do_date}' and activity_id is not null
    ) as order_detail on activity.activity_id = order_detail.activity_id
    group by activity.activity_id,        activity.activity_name, activity.activity_type_code,
             activity.activity_type_name, activity.start_time;
"

dws_traffic_page_visitor_page_view_nd="
    insert overwrite table ${HIVE_DATA_BASE}.dws_traffic_page_visitor_page_view_nd partition (dt = '${do_date}')
    select mid_id,
           brand,
           model,
           operate_system,
           page_id,
           sum(if(dt >= date_add('${do_date}', -6), during_time_1d, 0)) as during_time_7d,
           sum(if(dt >= date_add('${do_date}', -6), view_count_1d,  0)) as view_count_7d,
           sum(during_time_1d)                                          as during_time_30d,
           sum(view_count_1d)                                           as view_count_30d
    from ${HIVE_DATA_BASE}.dws_traffic_page_visitor_page_view_1d
    where dt >= date_add('${do_date}', -29) and dt <= '${do_date}'
    group by mid_id, brand, model, operate_system, page_id;
"

dws_trade_user_order_td="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_order_td partition (dt = '${do_date}')
    select user_id,
           min(dt)                        as order_date_first,
           max(dt)                        as order_date_last,
           sum(order_count_1d)            as order_count_td,
           sum(order_num_1d)              as order_num_td,
           sum(order_original_amount_1d)  as original_amount_td,
           sum(activity_reduce_amount_1d) as activity_reduce_amount_td,
           sum(coupon_reduce_amount_1d)   as coupon_reduce_amount_td,
           sum(order_total_amount_1d)     as total_amount_td
    from ${HIVE_DATA_BASE}.dws_trade_user_order_1d
    group by user_id;
"

dws_trade_user_payment_td="
    insert overwrite table ${HIVE_DATA_BASE}.dws_trade_user_payment_td partition (dt = '${do_date}')
    select user_id,
           min(dt)                as payment_date_first,
           max(dt)                as payment_date_last,
           sum(payment_count_1d)  as payment_count_td,
           sum(payment_num_1d)    as payment_num_td,
           sum(payment_amount_1d) as payment_amount_td
    from ${HIVE_DATA_BASE}.dws_trade_user_payment_1d
    group by user_id;
"

dws_user_user_login_td="
    insert overwrite table ${HIVE_DATA_BASE}.dws_user_user_login_td partition (dt = '${do_date}')
    select user_.id                                                                 as user_id, 
           nvl(login.login_date_last, date_format(user_.create_time, 'yyyy-MM-dd')) as login_date_last, 
           nvl(login.login_count_td,  1)                                            as login_count_td
    from 
    (
        select id, 
               create_time 
        from ${HIVE_DATA_BASE}.dim_user_zip 
        where dt = '9999-12-31'
    ) as user_ left join 
    (
        select user_id, 
               max(dt) login_date_last, 
               count(*) login_count_td
        from ${HIVE_DATA_BASE}.dwd_user_login_inc
        group by user_id
    ) as login on user_.id = login.user_id;
"


# 执行 Hive Sql
function execute_hive_sql()
{
    sql="$*"
    echo "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${HIVE_HOME}/bin/hive -e "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}


printf "\n============================================= 数据加载开始 =============================================\n"
case $1 in
    dws_trade_province_order_1d)
        execute_hive_sql "${dws_trade_province_order_1d}"
    ;;
    
    dws_trade_user_cart_add_1d)
        execute_hive_sql "${dws_trade_user_cart_add_1d}"
    ;;
    
    dws_trade_user_order_1d)
        execute_hive_sql "${dws_trade_user_order_1d}"
    ;;
    
    dws_trade_user_order_refund_1d)
        execute_hive_sql "${dws_trade_user_order_refund_1d}"
    ;;
    
    dws_trade_user_payment_1d)
        execute_hive_sql "${dws_trade_user_payment_1d}"
    ;;
    
    dws_trade_user_sku_order_1d)
        execute_hive_sql "${dws_trade_user_sku_order_1d}"
    ;;
    
    dws_trade_user_sku_order_refund_1d)
        execute_hive_sql "${dws_trade_user_sku_order_refund_1d}"
    ;;
    
    dws_traffic_page_visitor_page_view_1d)
        execute_hive_sql "${dws_traffic_page_visitor_page_view_1d}"
    ;;
    
    dws_traffic_session_page_view_1d)
        execute_hive_sql "${dws_traffic_session_page_view_1d}"
    ;;
    
    dws_trade_activity_order_nd)
        execute_hive_sql "${dws_trade_activity_order_nd}"
    ;;
    
    dws_trade_coupon_order_nd)
        execute_hive_sql "${dws_trade_coupon_order_nd}"
    ;;
    
    dws_trade_province_order_nd)
        execute_hive_sql "${dws_trade_province_order_nd}"
    ;;
    
    dws_trade_user_cart_add_nd)
        execute_hive_sql "${dws_trade_user_cart_add_nd}"
    ;;
    
    dws_trade_user_order_nd)
        execute_hive_sql "${dws_trade_user_order_nd}"
    ;;
    
    dws_trade_user_order_refund_nd)
        execute_hive_sql "${dws_trade_user_order_refund_nd}"
    ;;
    
    dws_trade_user_payment_nd)
        execute_hive_sql "${dws_trade_user_payment_nd}"
    ;;
    
    dws_trade_user_sku_order_nd)
        execute_hive_sql "${dws_trade_user_sku_order_nd}"
    ;;
    
    dws_trade_user_sku_order_refund_nd)
        execute_hive_sql "${dws_trade_user_sku_order_refund_nd}"
    ;;
    
    dws_traffic_page_visitor_page_view_nd)
        execute_hive_sql "${dws_traffic_page_visitor_page_view_nd}"
    ;;
    
    dws_trade_user_order_td)
        execute_hive_sql "${dws_trade_user_order_td}"
    ;;
    
    dws_trade_user_payment_td)
        execute_hive_sql "${dws_trade_user_payment_td}"
    ;;
    
    dws_user_user_login_td)
        execute_hive_sql "${dws_user_user_login_td}"
    ;;
    
    1d)
        execute_hive_sql "${dws_trade_province_order_1d}"            "${dws_trade_user_cart_add_1d}"             \
                         "${dws_trade_user_order_1d}"                "${dws_trade_user_order_refund_1d}"         \
                         "${dws_trade_user_payment_1d}"              "${dws_trade_user_sku_order_1d}"            \
                         "${dws_trade_user_sku_order_refund_1d}"     "${dws_traffic_page_visitor_page_view_1d}"  \
                         "${dws_traffic_session_page_view_1d}"
    ;;
    
    nd)
        execute_hive_sql "${dws_trade_activity_order_nd}"         "${dws_trade_coupon_order_nd}"             \
                         "${dws_trade_province_order_nd}"         "${dws_trade_user_cart_add_nd}"            \
                         "${dws_trade_user_order_nd}"             "${dws_trade_user_order_refund_nd}"        \
                         "${dws_trade_user_payment_nd}"           "${dws_trade_user_sku_order_nd}"           \
                         "${dws_trade_user_sku_order_refund_nd}"  "${dws_traffic_page_visitor_page_view_nd}"
    ;;
    
    td)
        execute_hive_sql "${dws_trade_user_order_td}"  "${dws_trade_user_payment_td}"  "${dws_user_user_login_td}"
    ;;
    
    all)
        execute_hive_sql "${dws_trade_province_order_1d}"            "${dws_trade_user_cart_add_1d}"             \
                         "${dws_trade_user_order_1d}"                "${dws_trade_user_order_refund_1d}"         \
                         "${dws_trade_user_payment_1d}"              "${dws_trade_user_sku_order_1d}"            \
                         "${dws_trade_user_sku_order_refund_1d}"     "${dws_traffic_page_visitor_page_view_1d}"  \
                         "${dws_traffic_session_page_view_1d}"       "${dws_trade_activity_order_nd}"            \
                         "${dws_trade_coupon_order_nd}"              "${dws_trade_province_order_nd}"            \
                         "${dws_trade_user_cart_add_nd}"             "${dws_trade_user_order_nd}"                \
                         "${dws_trade_user_order_refund_nd}"         "${dws_trade_user_payment_nd}"              \
                         "${dws_trade_user_sku_order_nd}"            "${dws_trade_user_sku_order_refund_nd}"     \
                         "${dws_traffic_page_visitor_page_view_nd}"  "${dws_trade_user_order_td}"               \
                         "${dws_trade_user_payment_td}"              "${dws_user_user_login_td}"
    ;;

    *)
        echo "    脚本可传入两个参数，使用方法：/path/$(basename $0) arg1 arg2：                                     "
        echo "        arg1：表名，必填，如下表所示；arg2：日期（yyyy-mm-dd） "
        echo "        +-----------------------------------------+---------------------------------------------+ " 
        echo "        |                 参   数                 |                   描   述                   | "
        echo "        +-----------------------------------------+---------------------------------------------+ " 
        echo "        |  dws_trade_province_order_1d            |  交易域省份粒度订单最近 1 日汇总表          | "
        echo "        |  dws_trade_user_cart_add_1d             |  交易域用户粒度加购最近 1 日汇总表          | "    
        echo "        |  dws_trade_user_order_1d                |  交易域用户粒度订单最近 1 日汇总表          | "    
        echo "        |  dws_trade_user_order_refund_1d         |  交易域用户粒度退单最近 1 日汇总表          | "    
        echo "        |  dws_trade_user_payment_1d              |  交易域用户粒度支付最近 1 日汇总表          | "        
        echo "        |  dws_trade_user_sku_order_1d            |  交易域用户商品粒度订单最近 1 日汇总表      | "    
        echo "        |  dws_trade_user_sku_order_refund_1d     |  交易域用户商品粒度退单最近 1 日汇总表      | "    
        echo "        |  dws_traffic_page_visitor_page_view_1d  |  流量域访客页面粒度页面浏览最近 1 日汇总表  | "    
        echo "        |  dws_traffic_session_page_view_1d       |  流量域会话粒度页面浏览最近 1 日汇总表      | "    
        echo "        |  dws_trade_activity_order_nd            |  交易域活动粒度订单最近 N 日汇总表          | "    
        echo "        |  dws_trade_coupon_order_nd              |  交易域优惠券粒度订单最近 N 日汇总表        | "    
        echo "        |  dws_trade_province_order_nd            |  交易域省份粒度订单最近 N 日汇总表          | "    
        echo "        |  dws_trade_user_cart_add_nd             |  交易域用户粒度加购最近 N 日汇总表          | "    
        echo "        |  dws_trade_user_order_nd                |  交易域用户粒度订单最近 N 日汇总表          | "    
        echo "        |  dws_trade_user_order_refund_nd         |  交易域用户粒度退单最近 N 日汇总表          | "    
        echo "        |  dws_trade_user_payment_nd              |  交易域用户粒度支付最近 N 日汇总表          | "    
        echo "        |  dws_trade_user_sku_order_nd            |  交易域用户商品粒度订单最近 N 日汇总表      | "    
        echo "        |  dws_trade_user_sku_order_refund_nd     |  交易域用户商品粒度退单最近 N 日汇总表      | "        
        echo "        |  dws_traffic_page_visitor_page_view_nd  |  流量域访客页面粒度页面浏览最近 N 日汇总表  | "        
        echo "        |  dws_trade_user_order_td                |  交易域用户粒度订单历史至今汇总表           | "        
        echo "        |  dws_trade_user_payment_td              |  交易域用户粒度支付历史至今汇总表           | "        
        echo "        |  dws_user_user_login_td                 |  用户域用户粒度登录历史至今汇总表           | "               
        echo "        |  1d                                     |  所有 DWD 层 最近 1 日汇总表                | "    
        echo "        |  nd                                     |  所有 DWD 层 最近 N 日汇总表                | "    
        echo "        |  td                                     |  所有 DWD 层 历史至今汇总表                 | "    
        echo "        |  all                                    |  所有 DWD 表                                | "    
        echo "        +-----------------------------------------+---------------------------------------------+ " 
    ;;
esac
    
printf "================================================ 运行结束 ================================================\n\n"
exit 0

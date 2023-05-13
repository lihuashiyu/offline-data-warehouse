#!/usr/bin/env bash


# =========================================================================================
#    FileName      ：  ods-dim.sh
#    CreateTime    ：  2023-03-26 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  ods-dim.sh 被用于 ==> 将 ODS 层数据加载到 DIM 
# =========================================================================================
    
    
# 定义变量方便修改
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
HIVE_HOME=/opt/apache/hive                                 # Hive 的安装位置
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
LOG_FILE="dws-ads-$(date +%F).log"                         # 执行日志


if [ -n "$2" ]; then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi

ads_traffic_stats_by_channel="
    insert overwrite table ${HIVE_DATA_BASE}.ads_traffic_stats_by_channel
    select dt, recent_days, channel, uv_count, avg_duration_sec, avg_page_count, sv_count, bounce_rate 
    from ads_traffic_stats_by_channel as traffic_stats
    union
    select '${do_date}'                                                                               as dt,
           recent_days,                       
           channel,                       
           cast(count(distinct (mid_id)) as bigint)                                                   as uv_count,
           cast(avg(during_time_1d) / 1000 as bigint)                                                 as avg_duration_sec,
           cast(avg(page_count_1d) as bigint)                                                         as avg_page_count,
           cast(count(*) as bigint)                                                                   as sv_count,
           cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as decimal(16, 2))                        as bounce_rate
    from ${HIVE_DATA_BASE}.dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp as recent_days 
    where dt >= date_add('${do_date}', -recent_days + 1)
    group by recent_days, channel;
"

ads_page_path="
    insert overwrite table ${HIVE_DATA_BASE}.ads_page_path
    select dt, recent_days, source, target, path_count
    from ads_page_path as page_path
    union
    select '${do_date}'                  as dt, 
           page_view.recent_days, 
           page_view.source, 
           nvl(page_view.target, 'null') as target, 
           count(*)                      as path_count
    from 
    (
        select page.recent_days, 
               concat('step-', page.rn,     ':', page.page_id)      as source, 
               concat('step-', page.rn + 1, ':', page.next_page_id) as target
        from 
        (
            select recent_days,
                   page_id,
                   lead(page_id, 1, null) over (partition by session_id, recent_days)                    as next_page_id,
                   row_number()           over (partition by session_id, recent_days order by view_time) as rn
            from ${HIVE_DATA_BASE}.dwd__traffic_page_view_inc lateral view explode(array(1, 7, 30)) tmp  as recent_days
            where dt >= date_add('${do_date}', -recent_days + 1)
        ) as page
    ) as page_view 
    group by page_view.recent_days, page_view.source, page_view.target;
"

ads_user_change="
    insert overwrite table ${HIVE_DATA_BASE}.ads_user_change
    select dt, user_churn_count, user_back_count
    from ads_user_change as user_change
    union
    select user_login.dt,
           user_login.user_churn_count,
           back.user_back_count
    from 
    (
        select '${do_date}' as dt, 
               count(*)     as user_churn_count
        from ${HIVE_DATA_BASE}.dws_user_user_login_td 
        where dt = '${do_date}' and login_date_last = date_add('${do_date}', -7)
    ) as user_login join 
    (
        select '${do_date}' as dt, 
               count(*)     as user_back_count
        from 
        (
            select user_id, 
                   login_date_last
            from ${HIVE_DATA_BASE}.dws_user_user_login_td
            where dt = '${do_date}' and login_date_last = '${do_date}'         -- 今日活跃的用户
        ) as login_1 join 
        (
            select user_id, 
                   login_date_last as login_date_previous
            from ${HIVE_DATA_BASE}.dws_user_user_login_td
            where dt = date_add('${do_date}', -1)                              -- 找出今日活跃用户的上次活跃日期
        ) as login_2 on login_1.user_id = login_2.user_id
        where datediff(login_1.login_date_last, login_2.login_date_previous) >= 8
    ) as back on user_login.dt = back.dt;
"

ads_user_retention="
    insert overwrite table ${HIVE_DATA_BASE}.ads_user_retention
    select dt, create_date, retention_day, retention_count, new_user_count, retention_rate
    from ads_user_retention as user_retention
    union
    select '${do_date}'                                                                                 as dt,
           register.login_date_first                                                                    as create_date,
           datediff('${do_date}', register.login_date_first)                                            as retention_day,
           sum(if(login.login_date_last = '${do_date}', 1, 0))                                          as retention_count,
           count(*)                                                                                     as new_user_count,
           cast(sum(if(login.login_date_last = '${do_date}', 1, 0)) / count(*) * 100 as decimal(16, 2)) as retention_rate
    from 
    (
        select user_id, 
               date_id  as login_date_first
        from ${HIVE_DATA_BASE}.dwd__user_register_inc
        where dt >= date_add('${do_date}', -7) and dt < '${do_date}'
    ) as register join 
    (
        select user_id, 
               login_date_last 
        from ${HIVE_DATA_BASE}.dws_user_user_login_td 
        where dt = '${do_date}'
    ) as login on register.user_id = login.user_id
    group by register.login_date_first;
"

ads_user_stats="
    insert overwrite table ${HIVE_DATA_BASE}.ads_user_stats
    select dt, recent_days, new_user_count, active_user_count
    from ads_user_stats as stats
    union
    select '${do_date}'             as dt, 
           register.recent_days,
           register.new_user_count,
           login.active_user_count
    from 
    (
        select recent_days, 
               count(*)     as new_user_count
        from ${HIVE_DATA_BASE}.dwd__user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
        where dt >= date_add('${do_date}', - recent_days + 1)
        group by recent_days
    ) as register join 
    (
        select recent_days, 
               count(*)    as active_user_count
        from ${HIVE_DATA_BASE}.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
        where dt = '${do_date}' and login_date_last >= date_add('${do_date}', - recent_days + 1)
        group by recent_days
    ) as login on register.recent_days = login.recent_days;
";

ads_user_action="
    insert overwrite table ${HIVE_DATA_BASE}.ads_user_action
    select dt, recent_days, home_count, good_detail_count, cart_count, order_count, payment_count
    from ads_user_action as action
    union
    select '${do_date}'            as dt, 
           page.recent_days,
           page.home_count,
           page.good_detail_count, 
           cart.cart_count,
           user_order.order_count,
           pay.payment_count
    from 
    (
        select 1                                      as recent_days,
               sum(if(page_id = 'home',        1, 0)) as home_count,
               sum(if(page_id = 'good_detail', 1, 0)) as good_detail_count
        from ${HIVE_DATA_BASE}.dws_traffic_page_visitor_page_view_1d
        where dt = '${do_date}' and page_id in ('home', 'good_detail')
        union all
        select visitor_view.recent_days,
               sum(if(visitor_view.page_id = 'home'        and visitor_view.view_count > 0, 1, 0)) as home_count,
               sum(if(visitor_view.page_id = 'good_detail' and visitor_view.view_count > 0, 1, 0)) as good_detail_count
        from 
        (
            select recent_days,
                   page_id,
                   case recent_days 
                       when 7 then  view_count_7d 
                       when 30 then view_count_30d 
                    end                                                                       as view_count
            from ${HIVE_DATA_BASE}.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}' and page_id in ('home', 'good_detail')
        ) as visitor_view group by visitor_view.recent_days
    ) as page join 
    (
        select 1        as recent_days, 
               count(*) as cart_count
        from ${HIVE_DATA_BASE}.dws_trade_user_cart_add_1d
        where dt = '${do_date}'
        union all
        select user_cart.recent_days, 
               sum(if(user_cart.cart_count > 0, 1, 0)) as cart_count
        from 
        (
            select recent_days,
                   case recent_days
                       when 7  then cart_add_count_7d
                       when 30 then cart_add_count_30d
                   end                                                                               as cart_count
            from ${HIVE_DATA_BASE}.dws_trade_user_cart_add_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_cart group by recent_days
    ) as cart 
        on page.recent_days = cart.recent_days
    join 
    (
        select 1        as recent_days, 
               count(*) as order_count
        from ${HIVE_DATA_BASE}.dws_trade_user_order_1d
        where dt = '${do_date}'
        union all
        select trade_user_order.recent_days, 
               sum(if(trade_user_order.order_count > 0, 1, 0)) as order_count
        from 
        (
            select recent_days,
                   case recent_days 
                       when 7 then order_count_7d 
                       when 30 then order_count_30d 
                   end                                                                            as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as trade_user_order group by trade_user_order.recent_days
    ) as user_order 
        on page.recent_days = user_order.recent_days 
    join 
    (
        select 1         as recent_days, 
               count(*)  as payment_count
        from ${HIVE_DATA_BASE}.dws_trade_user_payment_1d
        where dt = '${do_date}'
        union all
        select user_payment.recent_days, 
               sum(if(user_payment.order_count > 0, 1, 0)) as payment_count
        from 
        (
            select recent_days,
                   case recent_days
                       when 7  then payment_count_7d
                       when 30 then payment_count_30d
                   end                                                                              as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_payment_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_payment group by user_payment.recent_days
    ) as pay on page.recent_days = pay.recent_days;
"

ads_new_buyer_stats="
    insert overwrite table ${HIVE_DATA_BASE}.ads_new_buyer_stats
    select dt, recent_days, new_order_user_count, new_payment_user_count 
    from ads_new_buyer_stats as buyer_stats
    union
    select '${do_date}'                     as dt, 
           user_order.recent_days,
           user_order.new_order_user_count, 
           pay.new_payment_user_count
    from 
    (
        select recent_days,
               sum(if(order_date_first >= date_add('${do_date}', -recent_days + 1), 1, 0))       as new_order_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days
        where dt = '${do_date}'
        group by recent_days
    ) as user_order join 
    (
        select recent_days,
               sum(if(payment_date_first >= date_add('${do_date}', -recent_days + 1), 1, 0))       as new_payment_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_payment_td lateral view explode(array(1, 7, 30)) tmp as recent_days
        where dt = '${do_date}'
        group by recent_days
    ) pay on user_order.recent_days = pay.recent_days;
"

ads_repeat_purchase_by_tm="
    insert overwrite table ${HIVE_DATA_BASE}.ads_repeat_purchase_by_tm
    select dt, recent_days, tm_id, tm_name, order_repeat_rate
    from ads_repeat_purchase_by_tm as repeat_purchase
    union
    select '${do_date}'                                                                   as dt,
           sku.recent_days,
           sku.tm_id,
           sku.tm_name,
           cast(sum(if(sku.order_count >= 2, 1, 0)) / sum(if(sku.order_count >= 1, 1, 0)) as decimal(16, 2))
    from 
    (
        select user_sku_order.recent_days, 
               user_sku_order.tm_id, 
               user_sku_order.tm_name, 
               sum(user_sku_order.order_count) as order_count
        from 
        (
            select recent_days,
                   user_id,
                   tm_id,
                   tm_name,
                   case recent_days 
                       when 7  then order_count_7d 
                       when 30 then order_count_30d 
                   end                                                                                as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_sku_order 
        group by user_sku_order.recent_days, user_sku_order.user_id, user_sku_order.tm_id, user_sku_order.tm_name
    ) as sku 
    group by sku.recent_days, sku.tm_id, sku.tm_name;
"

ads_trade_stats_by_tm="
    insert overwrite table ${HIVE_DATA_BASE}.ads_trade_stats_by_tm
    select dt, recent_days, tm_id, tm_name, order_count, order_user_count, order_refund_count, order_refund_user_count 
    from ads_trade_stats_by_tm as trade_stats
    union
    select '${do_date}'                                            as dt,
           nvl(sku.recent_days,                refund.recent_days) as recent_days,
           nvl(sku.tm_id,                      refund.tm_id)       as tm_id,
           nvl(sku.tm_name,                    refund.tm_name)     as tm_name,
           nvl(sku.order_count,                0)                  as order_count,
           nvl(sku.order_user_count,           0)                  as order_user_count,
           nvl(refund.order_refund_count,      0)                  as order_refund_count,
           nvl(refund.order_refund_user_count, 0)                  as order_refund_user_count
    from 
    (
        select 1                         as recent_days,
               tm_id,
               tm_name,
               sum(order_count_1d)       as order_count,
               count(distinct (user_id)) as order_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_1d
        where dt = '${do_date}'
        group by tm_id, tm_name
        union all
        select user_sku_order.recent_days, 
               user_sku_order.tm_id, 
               user_sku_order.tm_name, 
               sum(user_sku_order.order_count)                                                    as order_count, 
               count(distinct (if(user_sku_order.order_count > 0, user_sku_order.user_id, null))) as order_user_count
        from 
        (
            select recent_days,
                   user_id,
                   tm_id,
                   tm_name,
                   case recent_days 
                       when 7 then  order_count_7d 
                       when 30 then order_count_30d 
                   end                                                                                as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_sku_order 
        group by user_sku_order.recent_days, user_sku_order.tm_id, user_sku_order.tm_name
    ) as sku full outer join 
    (
        select 1                          as recent_days,
               tm_id,
               tm_name,
               sum(order_refund_count_1d) as order_refund_count,
               count(distinct (user_id))  as order_refund_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_1d
        where dt = '${do_date}'
        group by tm_id, tm_name
        union all
        select order_refund.recent_days, 
               order_refund.tm_id, 
               order_refund.tm_name, 
               sum(order_refund.order_refund_count)                          as order_refund_count, 
               count(if(order_refund.order_refund_count > 0, user_id, null)) as order_refund_user_count
        from 
        (
            select recent_days,
                   user_id,
                   tm_id,
                   tm_name,
                   case recent_days
                       when 7  then order_refund_count_7d
                       when 30 then order_refund_count_30d
                   end                                                                                       as order_refund_count
            from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as order_refund 
        group by order_refund.recent_days, order_refund.tm_id, order_refund.tm_name
    ) as refund 
        on sku.recent_days  = refund.recent_days 
            and sku.tm_id   = refund.tm_id 
            and sku.tm_name = refund.tm_name;
"

ads_trade_stats_by_cate="
    insert overwrite table ${HIVE_DATA_BASE}.ads_trade_stats_by_cate
    select dt, recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name,
           order_count, order_user_count, order_refund_count, order_refund_user_count 
    from ads_trade_stats_by_cate as trade_stats
    union 
    select '${do_date}'                                               as dt,
           nvl(odr.recent_days,                refund.recent_days)    as recent_days,
           nvl(odr.category1_id,               refund.category1_id)   as category1_id,
           nvl(odr.category1_name,             refund.category1_name) as category1_name,
           nvl(odr.category2_id,               refund.category2_id)   as category2_id,
           nvl(odr.category2_name,             refund.category2_name) as category2_name,
           nvl(odr.category3_id,               refund.category3_id)   as category3_id,
           nvl(odr.category3_name,             refund.category3_name) as category3_name,
           nvl(odr.order_count,                0)                     as order_count,
           nvl(odr.order_user_count,           0)                     as order_user_count,
           nvl(refund.order_refund_count,      0)                     as order_refund_count,
           nvl(refund.order_refund_user_count, 0)                     as order_refund_user_count
    from 
    (
        select 1                         as recent_days,
               category1_id,
               category1_name,
               category2_id,
               category2_name,
               category3_id,
               category3_name,
               sum(order_count_1d)       as order_count,
               count(distinct (user_id)) as order_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_1d
        where dt = '${do_date}'
        group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
        union all
        select user_sku_order.recent_days,
               user_sku_order.category1_id,
               user_sku_order.category1_name,
               user_sku_order.category2_id,
               user_sku_order.category2_name,
               user_sku_order.category3_id,
               user_sku_order.category3_name,
               sum(user_sku_order.order_count)                                                     as order_count,
               count(distinct (if(user_sku_order.order_count > 0, user_sku_order.user_id, null)))  as order_user_count
        from 
        (
            select recent_days,
                   user_id,
                   category1_id,
                   category1_name,
                   category2_id,
                   category2_name,
                   category3_id,
                   category3_name,
                   case recent_days 
                       when 7  then order_count_7d 
                       when 30 then order_count_30d 
                   end                                                                                as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_sku_order 
        group by user_sku_order.recent_days,   user_sku_order.category1_id,   user_sku_order.category1_name, 
                 user_sku_order.category2_id,  user_sku_order.category2_name, user_sku_order.category3_id, 
                 user_sku_order.category3_name
    ) as odr full outer join 
    (
        select 1                          as recent_days,
               category1_id,
               category1_name,
               category2_id,
               category2_name,
               category3_id,
               category3_name,
               sum(order_refund_count_1d) as order_refund_count,
               count(distinct (user_id))  as order_refund_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_1d
        where dt = '${do_date}'
        group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
        union all
        select sku_order_refund.recent_days,
               sku_order_refund.category1_id,
               sku_order_refund.category1_name,
               sku_order_refund.category2_id,
               sku_order_refund.category2_name,
               sku_order_refund.category3_id,
               sku_order_refund.category3_name,
               sum(sku_order_refund.order_refund_count)                                                      as order_refund_count,
               count(distinct (if(sku_order_refund.order_refund_count > 0, sku_order_refund.user_id, null))) as order_refund_user_count
        from 
        (
            select recent_days,
                   user_id,
                   category1_id,
                   category1_name,
                   category2_id,
                   category2_name,
                   category3_id,
                   category3_name,
                   case recent_days
                       when 7  then order_refund_count_7d
                       when 30 then order_refund_count_30d
                   end                                                                     as order_refund_count
            from ${HIVE_DATA_BASE}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as sku_order_refund 
        group by sku_order_refund.recent_days,  sku_order_refund.category1_id,   sku_order_refund.category1_name,
                 sku_order_refund.category2_id, sku_order_refund.category2_name, sku_order_refund.category3_id,
                 sku_order_refund.category3_name
    ) as refund on odr.recent_days    = refund.recent_days    
               and odr.category1_id   = refund.category1_id 
               and odr.category1_name = refund.category1_name 
               and odr.category2_id   = refund.category2_id 
               and odr.category2_name = refund.category2_name 
               and odr.category3_id   = refund.category3_id 
               and odr.category3_name = refund.category3_name;
"

ads_sku_cart_num_top3_by_cate="
    insert overwrite table ${HIVE_DATA_BASE}.ads_sku_cart_num_top3_by_cate
    select dt, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, sku_id, sku_name, cart_num, rk 
    from ads_sku_cart_num_top3_by_cate as sku_cart_num
    union
    select '${do_date}'                   as dt,
           cart_num_top.category1_id,
           cart_num_top.category1_name,
           cart_num_top.category2_id,
           cart_num_top.category2_name,
           cart_num_top.category3_id,
           cart_num_top.category3_name,
           cart_num_top.sku_id,
           cart_num_top.sku_name,
           cart_num_top.cart_num,
           cart_num_top.rk
    from 
    (
        select cart.sku_id,
               sku.sku_name,
               sku.category1_id,
               sku.category1_name,
               sku.category2_id,
               sku.category2_name,
               sku.category3_id,
               sku.category3_name,
               cart.cart_num,
               rank() over (partition by sku.category1_id, sku.category2_id, sku.category3_id order by cart.cart_num desc) as rk
        from 
        (
            select sku_id, 
                   sum(sku_num)  as cart_num
            from ${HIVE_DATA_BASE}.dwd__trade_cart_full
            where dt = '${do_date}'
            group by sku_id
        ) as cart left join 
        (
            select id,
                   sku_name,
                   category1_id,
                   category1_name,
                   category2_id,
                   category2_name,
                   category3_id,
                   category3_name
            from ${HIVE_DATA_BASE}.dim__sku_full
            where dt = '${do_date}'
        ) as sku on cart.sku_id = sku.id
    ) as cart_num_top 
    where cart_num_top.rk <= 3;
"

ads_trade_stats="
    insert overwrite table ${HIVE_DATA_BASE}.ads_trade_stats
    select dt, recent_days, order_total_amount, order_count, order_user_count, order_refund_count, order_refund_user_count
    from ads_trade_stats as trade_stats
    union
    select '${do_date}'                      as dt,
           odr.recent_days,
           odr.order_total_amount,
           odr.order_count,
           odr.order_user_count,
           refund.order_refund_count,
           refund.order_refund_user_count
    from 
    (
        select 1                          as recent_days,
               sum(order_total_amount_1d) as order_total_amount,
               sum(order_count_1d)        as order_count,
               count(*)                   as order_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_order_1d
        where dt = '${do_date}'
        union all
        select user_order.recent_days, 
               sum(user_order.order_total_amount)        as order_total_amount, 
               sum(user_order.order_count)               as order_count,  
               sum(if(user_order.order_count > 0, 1, 0)) as order_user_count
        from 
        (
            select recent_days,
                  case recent_days
                      when 7  then order_total_amount_7d
                      when 30 then order_total_amount_30d
                  end                                                                             as order_total_amount,
                  case recent_days 
                      when 7  then order_count_7d 
                      when 30 then order_count_30d 
                  end                                                                             as order_count
            from ${HIVE_DATA_BASE}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as user_order group by user_order.recent_days
    ) as odr join 
    (
        select 1                          as recent_days, 
               sum(order_refund_count_1d) as order_refund_count, 
               count(*)                   as order_refund_user_count
        from ${HIVE_DATA_BASE}.dws_trade_user_order_refund_1d
        where dt = '${do_date}'
        union all
        select order_refund.recent_days, 
               sum(order_refund.order_refund_count)               as order_refund_count, 
               sum(if(order_refund.order_refund_count > 0, 1, 0)) as order_refund_user_count
        from 
        (
            select recent_days,
                   case recent_days
                       when 7  then order_refund_count_7d
                       when 30 then order_refund_count_30d
                   end                                                                                   as order_refund_count
            from ${HIVE_DATA_BASE}.dws_trade_user_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
            where dt = '${do_date}'
        ) as order_refund group by order_refund.recent_days
    ) as refund on odr.recent_days = refund.recent_days;
"

ads_order_by_province="
    insert overwrite table ${HIVE_DATA_BASE}.ads_order_by_province
    select dt, recent_days, province_id, province_name, area_code, iso_code, iso_code_3166_2, order_count, order_total_amount
    from ads_order_by_province  as order_province
    union
    select '${do_date}'             as dt,
           1                        as recent_days,
           province_id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2,
           order_count_1d           as order_count,
           order_total_amount_1d    as order_total_amount
    from ${HIVE_DATA_BASE}.dws_trade_province_order_1d
    where dt = '${do_date}'
    union all
    select '${do_date}'                                                                       as dt,
           recent_days,
           province_id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2,
           if(recent_days = 7, order_count_7d,        order_count_30d)                        as order_count,
           if(recent_days = 7, order_total_amount_7d, order_total_amount_30d)                 as order_total_amount
    from ${HIVE_DATA_BASE}.dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days
    where dt = '${do_date}';
"

ads_coupon_stats="
    insert overwrite table ${HIVE_DATA_BASE}.ads_coupon_stats
    select dt, coupon_id, coupon_name, start_date, rule_name, reduce_rate
    from ads_coupon_stats as coupon_stats
    union
    select '${do_date}'                                                           as dt,
           coupon_id,
           coupon_name,
           start_date,
           coupon_rule                                                            as rule_name,
           cast(coupon_reduce_amount_30d / original_amount_30d as decimal(16, 2)) as reduce_rate
    from ${HIVE_DATA_BASE}.dws_trade_coupon_order_nd
    where dt = '${do_date}';
"

ads_activity_stats="
    insert overwrite table ${HIVE_DATA_BASE}.ads_activity_stats
    select dt, activity_id, activity_name, start_date, reduce_rate 
    from ads_activity_stats as activity
    union
    select '${do_date}'                                                             as dt,
           activity_id,
           activity_name,
           start_date,
           cast(activity_reduce_amount_30d / original_amount_30d as decimal(16, 2)) as reduce_rate
    from ${HIVE_DATA_BASE}.dws_trade_activity_order_nd
    where dt = '${do_date}';
"


# 执行 Hive Sql
function execute_hive_sql()
{
    sql="$*"
    echo "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${HIVE_HOME}/bin/hive -e "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}


printf "\n======================================== 数据加载开始 ========================================\n"
case $1 in
    ads_activity_stats)
        execute_hive_sql "${ads_activity_stats}"
    ;;
    
    ads_coupon_stats)
        execute_hive_sql "${ads_coupon_stats}"
    ;;
    
    ads_new_buyer_stats)
        execute_hive_sql "${ads_new_buyer_stats}"
    ;;
    
    ads_order_by_province)
        execute_hive_sql "${ads_order_by_province}"
    ;;
    
    ads_page_path)
        execute_hive_sql "${ads_page_path}"
    ;;
    
    ads_repeat_purchase_by_tm)
        execute_hive_sql "${ads_repeat_purchase_by_tm}"
    ;;
    
    ads_sku_cart_num_top3_by_cate)
        execute_hive_sql "${ads_sku_cart_num_top3_by_cate}"
    ;;
    
    ads_trade_stats)
        execute_hive_sql "${ads_trade_stats}"
    ;;
    
    ads_trade_stats_by_cate)
        execute_hive_sql "${ads_trade_stats_by_cate}"
    ;;
    
    ads_trade_stats_by_tm)
        execute_hive_sql "${ads_trade_stats_by_tm}"
    ;;
    
    ads_traffic_stats_by_channel)
        execute_hive_sql "${ads_traffic_stats_by_channel}"
    ;;
    
    ads_user_action)
        execute_hive_sql "${ads_user_action}"
    ;;
    
    ads_user_change)
        execute_hive_sql "${ads_user_change}"
    ;;
    
    ads_user_retention)
        execute_hive_sql "${ads_user_retention}"
    ;;
    
    ads_user_stats)
        execute_hive_sql "$ads_user_stats"
    ;;
    
    all)
        execute_hive_sql "${ads_activity_stats}"             "${ads_coupon_stats}"              "${ads_new_buyer_stats}"        \
                         "${ads_order_by_province}"          "${ads_page_path}"                 "${ads_repeat_purchase_by_tm}"  \
                         "${ads_sku_cart_num_top3_by_cate}"  "${ads_trade_stats}"               "${ads_trade_stats_by_cate}"    \
                         "${ads_trade_stats_by_tm}"          "${ads_traffic_stats_by_channel}"  "${ads_user_action}"            \
                         "${ads_user_change}"                "${ads_user_retention}"            "${ads_user_stats}"
    ;;
    
    *)
        echo "    脚本可传入两个参数，使用方法：/path/$(basename $0) arg1 [arg2] ： "
        echo "        arg1：表名，必填，如下表所示；arg2：日期（yyyy-mm-dd），可选，默认昨天 "
        echo "        +---------------------------------+----------------------------------+ "
        echo "        |             参   数             |              描  述              | "
        echo "        +---------------------------------+----------------------------------+ "
        echo "        |  ads_activity_stats             |  最近 30 天发布的活动的补贴率    | "    
        echo "        |  ads_coupon_stats               |  最近 30 天发布的优惠券的补贴率  | "    
        echo "        |  ads_new_buyer_stats            |  新增交易用户统计                | "    
        echo "        |  ads_order_by_province          |  各省份交易统计                  | "    
        echo "        |  ads_page_path                  |  路径分析(页面单跳)              | "    
        echo "        |  ads_repeat_purchase_by_tm      |  最近 7/30 日各品牌复购率        | "    
        echo "        |  ads_sku_cart_num_top3_by_cate  |  各分类商品购物车存量 Top3       | "    
        echo "        |  ads_trade_stats                |  交易综合统计                    | "    
        echo "        |  ads_trade_stats_by_cate        |  各品类商品交易统计              | "    
        echo "        |  ads_trade_stats_by_tm          |  各品牌商品交易统计              | "    
        echo "        |  ads_traffic_stats_by_channel   |  各渠道流量统计                  | "    
        echo "        |  ads_user_action                |  用户行为漏斗分析                | "    
        echo "        |  ads_user_change                |  用户变动统计                    | "    
        echo "        |  ads_user_retention             |  用户留存率                      | "    
        echo "        |  ads_user_stats                 |  用户新增活跃统计                | "      
        echo "        |  all                            |  所有 ADS 表                     | "   
        echo "        +---------------------------------+----------------------------------+ "
    ;;
esac

printf "======================================== 运行结束 ========================================\n\n"
exit 0

-- -------------------------------------------------------------------------------------------------
-- DWS 层设计要点：
--         DWS 层的设计参考指标体系
--         DWS 层的数据存储格式为 orc 列式存储 +snappy 压缩。
--         DWS 层表名的命名规范为 dws_数据域_统计粒度_业务过程_统计周期（1d/nd/td）
-- -------------------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度订单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_1d;
create external table if not exists dws_trade_user_sku_order_1d
(
    user_id                   string         comment '用户 ID',
    sku_id                    string         comment 'sku_ ID',
    sku_name                  string         comment 'sku名称',
    category1_id              string         comment '一级分类 ID',
    category1_name            string         comment '一级分类名称',
    category2_id              string         comment '一级分类 ID',
    category2_name            string         comment '一级分类名称',
    category3_id              string         comment '一级分类 ID',
    category3_name            string         comment '一级分类名称',
    tm_id                     string         comment '品牌 ID',
    tm_name                   string         comment '品牌名称',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_num_1d              bigint         comment '最近 1 日下单件数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近 1 日优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域用户商品粒度订单最近 1 日汇总事实表' partitioned by (dt string)
    stored as orc location '/warehouse/dws/dws_trade_user_sku_order_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
select user_id,
       id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count_1d,
       order_num_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d,
       dt
    from 
    (
        select dt,
               user_id,
               sku_id,
               count(*)                             order_count_1d,
               sum(sku_num)                         order_num_1d,
               sum(split_original_amount)           order_original_amount_1d,
               sum(nvl(split_activity_amount, 0.0)) activity_reduce_amount_1d,
               sum(nvl(split_coupon_amount, 0.0))   coupon_reduce_amount_1d,
               sum(split_total_amount)              order_total_amount_1d
        from dwd_trade_order_detail_inc
        group by dt, user_id, sku_id
    ) od left join 
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
       from dim_sku_full
       where dt = '2021-08-15'
    ) sku on od.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_1d partition (dt = '2020-06-15')
select user_id,
       id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count,
       order_num,
       order_original_amount,
       activity_reduce_amount,
       coupon_reduce_amount,
       order_total_amount
    from 
    (
        select user_id,
               sku_id,
               count(*)                             order_count,
               sum(sku_num)                         order_num,
               sum(split_original_amount)           order_original_amount,
               sum(nvl(split_activity_amount, 0.0)) activity_reduce_amount,
               sum(nvl(split_coupon_amount, 0.0))   coupon_reduce_amount,
               sum(split_total_amount)              order_total_amount
        from dwd_trade_order_detail_inc
        where dt = '2020-06-15'
        group by user_id, sku_id
    ) od left join 
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
        from dim_sku_full
        where dt = '2020-06-15'
    ) sku on od.sku_id = sku.id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度退单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_refund_1d;
create external table if not exists dws_trade_user_sku_order_refund_1d
(
    user_id                string         comment '用户 ID',
    sku_id                 string         comment 'sku_ ID',
    sku_name               string         comment 'sku名称',
    category1_id           string         comment '一级分类 ID',
    category1_name         string         comment '一级分类名称',
    category2_id           string         comment '一级分类 ID',
    category2_name         string         comment '一级分类名称',
    category3_id           string         comment '一级分类 ID',
    category3_name         string         comment '一级分类名称',
    tm_id                  string         comment '品牌 ID',
    tm_name                string         comment '品牌名称',
    order_refund_count_1d  bigint         comment '最近 1 日退单次数',
    order_refund_num_1d    bigint         comment '最近 1 日退单件数',
    order_refund_amount_1d decimal(16, 2) comment '最近 1 日退单金额'
) comment '交易域用户商品粒度退单最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_sku_order_refund_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt)
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
       order_refund_count,
       order_refund_num,
       order_refund_amount,
       dt
    from 
    (
        select dt,
               user_id,
               sku_id,
               count(*)           order_refund_count,
               sum(refund_num)    order_refund_num,
               sum(refund_amount) order_refund_amount
        from dwd_trade_order_refund_inc
        group by dt, user_id, sku_id
    ) od left join 
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
        from dim_sku_full
        where dt = '2021-08-15'
   ) sku on od.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt = '2020-06-15')
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
       order_refund_count,
       order_refund_num,
       order_refund_amount
    from 
    (
        select user_id,
               sku_id,
               count(*)           order_refund_count,
               sum(refund_num)    order_refund_num,
               sum(refund_amount) order_refund_amount
        from dwd_trade_order_refund_inc
        where dt = '2020-06-15'
        group by user_id, sku_id
   ) od left join 
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
        from dim_sku_full
        where dt = '2020-06-15'
    ) sku on od.sku_id = sku.id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度订单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_1d;
create external table if not exists dws_trade_user_order_1d
(
    user_id                   string         comment '用户 ID',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_num_1d              bigint         comment '最近 1 日下单商品件数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域用户粒度订单最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_order_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_1d partition (dt)
    select user_id,
           count(distinct (order_id)),
           sum(sku_num),
           sum(split_original_amount),
           sum(nvl(split_activity_amount, 0)),
           sum(nvl(split_coupon_amount, 0)),
           sum(split_total_amount),
           dt
        from dwd_trade_order_detail_inc
        group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_1d partition (dt = '2020-06-15')
    select user_id,
           count(distinct (order_id)),
           sum(sku_num),
           sum(split_original_amount),
           sum(nvl(split_activity_amount, 0)),
           sum(nvl(split_coupon_amount, 0)),
           sum(split_total_amount)
    from dwd_trade_order_detail_inc
    where dt = '2020-06-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度加购最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_cart_add_1d;
create external table if not exists dws_trade_user_cart_add_1d
(
    user_id           string comment '用户 ID',
    cart_add_count_1d bigint comment '最近 1 日加购次数',
    cart_add_num_1d   bigint comment '最近 1 日加购商品件数'
) comment '交易域用户粒度加购最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_cart_add_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
    select user_id, count(*), sum(sku_num), dt
    from dwd_trade_cart_add_inc
    group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '2020-06-15')
    select user_id, count(*), sum(sku_num)
    from dwd_trade_cart_add_inc
    where dt = '2020-06-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度支付最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_payment_1d;
create external table if not exists dws_trade_user_payment_1d
(
    user_id           string         comment '用户 ID',
    payment_count_1d  bigint         comment '最近 1 日支付次数',
    payment_num_1d    bigint         comment '最近 1 日支付商品件数',
    payment_amount_1d decimal(16, 2) comment '最近 1 日支付金额'
) comment '交易域用户粒度支付最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_payment_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_1d partition (dt)
    select user_id, count(distinct (order_id)), sum(sku_num), sum(split_payment_amount), dt
    from dwd_trade_pay_detail_suc_inc
    group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_payment_1d partition (dt = '2020-06-15')
    select user_id, count(distinct (order_id)), sum(sku_num), sum(split_payment_amount)
    from dwd_trade_pay_detail_suc_inc
    where dt = '2020-06-15'
    group by user_id;

-- -------------------------------------------------------------------------------------------------
-- 最近 1 交易域用户粒度退单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_refund_1d;
create external table if not exists dws_trade_user_order_refund_1d
(
    user_id                string comment '用户 ID',
    order_refund_count_1d  bigint comment '最近 1 日退单次数',
    order_refund_num_1d    bigint comment '最近 1 日退单商品件数',
    order_refund_amount_1d decimal(16, 2) comment '最近 1 日退单金额'
) comment '交易域用户粒度退单最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_order_refund_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_refund_1d partition (dt)
    select user_id,
           count(*)           order_refund_count,
           sum(refund_num)    order_refund_num,
           sum(refund_amount) order_refund_amount,
           dt
    from dwd_trade_order_refund_inc
    group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_refund_1d partition (dt = '2020-06-15')
    select user_id, 
           count(*), 
           sum(refund_num), 
           sum(refund_amount)
    from dwd_trade_order_refund_inc
    where dt = '2020-06-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域省份粒度订单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_province_order_1d;
create external table if not exists dws_trade_province_order_1d
(
    province_id               string         comment '省份 ID',
    province_name             string         comment '省份名称',
    area_code                 string         comment '地区编码',
    iso_code                  string         comment '旧版ISO-3166-2编码',
    iso_3166_2                string         comment '新版版ISO-3166-2编码',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近 1 日下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域省份粒度订单最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_province_order_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition (dt)
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       o.order_count_1d,
       o.order_original_amount_1d,
       o.activity_reduce_amount_1d,
       o.coupon_reduce_amount_1d,
       o.order_total_amount_1d,
       o.dt
    from 
    (
        select province_id,
               count(distinct (order_id))         order_count_1d,
               sum(split_original_amount)         order_original_amount_1d,
               sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
               sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
               sum(split_total_amount)            order_total_amount_1d,
               dt
        from dwd_trade_order_detail_inc
        group by province_id, dt
    ) o left join 
    (
        select id, 
               province_name, 
               area_code, 
               iso_code, 
               iso_3166_2
        from dim_province_full
        where dt = '2021-08-15'
    ) p on o.province_id = p.id;

-- 每日装载
insert overwrite table dws_trade_province_order_1d partition (dt = '2020-06-15')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
    from 
    (
        select province_id,
                 count(distinct (order_id))         order_count_1d,
                 sum(split_original_amount)         order_original_amount_1d,
                 sum(nvl(split_activity_amount, 0)) activity_reduce_amount_1d,
                 sum(nvl(split_coupon_amount, 0))   coupon_reduce_amount_1d,
                 sum(split_total_amount)            order_total_amount_1d
        from dwd_trade_order_detail_inc
        where dt = '2020-06-15'
        group by province_id
    ) o left join 
    (
        select id, 
               province_name, 
               area_code, 
               iso_code, 
               iso_3166_2
        from dim_province_full
        where dt = '2020-06-15'
    ) p on o.province_id = p.id;


-- -------------------------------------------------------------------------------------------------
-- 流量域会话粒度页面浏览最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_traffic_session_page_view_1d;
create external table if not exists dws_traffic_session_page_view_1d
(
    session_id     string comment '会话 ID',
    mid_id         string comment '设备 ID',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    version_code   string comment 'app版本号',
    channel        string comment '渠道',
    during_time_1d bigint comment '最近 1 日访问时长',
    page_count_1d  bigint comment '最近 1 日访问页面数'
) comment '流量域会话粒度页面浏览最近 1 日汇总表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_traffic_session_page_view_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_session_page_view_1d partition (dt = '2021-08-15')
    select session_id,
           mid_id,
           brand,
           model,
           operate_system,
           version_code,
           channel,
           sum(during_time),
           count(*)
    from dwd_traffic_page_view_inc
    where dt = '2021-08-15'
    group by session_id, mid_id, brand, model, operate_system, version_code, channel;


-- -------------------------------------------------------------------------------------------------
-- 流量域访客页面粒度页面浏览最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_traffic_page_visitor_page_view_1d;
create external table if not exists dws_traffic_page_visitor_page_view_1d
(
    mid_id         string comment '访客 ID',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    page_id        string comment '页面 ID',
    during_time_1d bigint comment '最近 1 日浏览时长',
    view_count_1d  bigint comment '最近 1 日访问次数'
) comment '流量域访客页面粒度页面浏览最近 1 日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_traffic_page_visitor_page_view_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2021-08-15')
    select mid_id, 
           brand, 
           model, 
           operate_system, 
           page_id, 
           sum(during_time), 
           count(*)
    from dwd_traffic_page_view_inc
    where dt = '2021-08-15'
    group by mid_id, brand, model, operate_system, page_id;

-- 最近 1 日汇总表，首日数据装载脚本：dwd_to_dws_1d_init.sh all 2021-08-15
-- 最近 1 日汇总表，每日数据装载脚本：dwd_to_dws_1d.sh all 2021-08-15



-- -------------------------------------------------------------------------------------------------
-- 最近 N 日汇总表：交易域用户商品粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_nd;
create external table if not exists dws_trade_user_sku_order_nd
(
    user_id                    string         comment '用户 ID',
    sku_id                     string         comment 'sku_ ID',
    sku_name                   string         comment 'sku名称',
    category1_id               string         comment '一级分类 ID',
    category1_name             string         comment '一级分类名称',
    category2_id               string         comment '一级分类 ID',
    category2_name             string         comment '一级分类名称',
    category3_id               string         comment '一级分类 ID',
    category3_name             string         comment '一级分类名称',
    tm_id                      string         comment '品牌 ID',
    tm_name                    string         comment '品牌名称',
    order_count_7d             string         comment '最近7日下单次数',
    order_num_7d               bigint         comment '最近7日下单件数',
    order_original_amount_7d   decimal(16, 2) comment '最近7日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近7日活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近7日优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近7日下单最终金额',
    order_count_30d            bigint         comment '最近30日下单次数',
    order_num_30d              bigint         comment '最近30日下单件数',
    order_original_amount_30d  decimal(16, 2) comment '最近30日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近30日活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近30日优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近30日下单最终金额'
) comment '交易域用户商品粒度订单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_sku_order_nd' 
    tblproperties ('orc.compress' = 'snappy');


insert overwrite table dws_trade_user_sku_order_nd partition (dt = '2021-08-15')
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
           sum(if(dt >= date_add('2021-08-15', -6), order_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_num_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d, 0)),
           sum(order_count_1d),
           sum(order_num_1d),
           sum(order_original_amount_1d),
           sum(activity_reduce_amount_1d),
           sum(coupon_reduce_amount_1d),
           sum(order_total_amount_1d)
    from dws_trade_user_sku_order_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, 
             category2_name, category3_id, category3_name, tm_id, tm_name;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度退单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_refund_nd;
create external table if not exists dws_trade_user_sku_order_refund_nd
(
    user_id                 string         comment '用户 ID',
    sku_id                  string         comment 'sku_ ID',
    sku_name                string         comment 'sku名称',
    category1_id            string         comment '一级分类 ID',
    category1_name          string         comment '一级分类名称',
    category2_id            string         comment '一级分类 ID',
    category2_name          string         comment '一级分类名称',
    category3_id            string         comment '一级分类 ID',
    category3_name          string         comment '一级分类名称',
    tm_id                   string         comment '品牌 ID',
    tm_name                 string         comment '品牌名称',
    order_refund_count_7d   bigint         comment '最近7日退单次数',
    order_refund_num_7d     bigint         comment '最近7日退单件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近7日退单金额',
    order_refund_count_30d  bigint         comment '最近30日退单次数',
    order_refund_num_30d    bigint         comment '最近30日退单件数',
    order_refund_amount_30d decimal(16, 2) comment '最近30日退单金额'
) comment '交易域用户商品粒度退单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_sku_order_refund_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_trade_user_sku_order_refund_nd partition (dt = '2021-08-15')
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
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_num_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_amount_1d, 0)),
           sum(order_refund_count_1d),
           sum(order_refund_num_1d),
           sum(order_refund_amount_1d)
        from dws_trade_user_sku_order_refund_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, 
             category2_name, category3_id, category3_name, tm_id, tm_name;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_nd;
create external table if not exists dws_trade_user_order_nd
(
    user_id                    string         comment '用户 ID',
    order_count_7d             bigint         comment '最近7日下单次数',
    order_num_7d               bigint         comment '最近7日下单商品件数',
    order_original_amount_7d   decimal(16, 2) comment '最近7日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近7日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近7日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近7日下单最终金额',
    order_count_30d            bigint         comment '最近30日下单次数',
    order_num_30d              bigint         comment '最近30日下单商品件数',
    order_original_amount_30d  decimal(16, 2) comment '最近30日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近30日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近30日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近30日下单最终金额'
) comment '交易域用户粒度订单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_order_nd partition (dt = '2021-08-15')
    select user_id,
           sum(if(dt >= date_add('2021-08-15', -6), order_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_num_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d, 0)),
           sum(order_count_1d),
           sum(order_num_1d),
           sum(order_original_amount_1d),
           sum(activity_reduce_amount_1d),
           sum(coupon_reduce_amount_1d),
           sum(order_total_amount_1d)
    from dws_trade_user_order_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度加购最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_cart_add_nd;
create external table if not exists dws_trade_user_cart_add_nd
(
    user_id            string comment '用户 ID',
    cart_add_count_7d  bigint comment '最近7日加购次数',
    cart_add_num_7d    bigint comment '最近7日加购商品件数',
    cart_add_count_30d bigint comment '最近30日加购次数',
    cart_add_num_30d   bigint comment '最近30日加购商品件数'
) comment '交易域用户粒度加购最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_cart_add_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_cart_add_nd partition (dt = '2021-08-15')
    select user_id,
           sum(if(dt >= date_add('2021-08-15', -6), cart_add_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), cart_add_num_1d, 0)),
           sum(cart_add_count_1d),
           sum(cart_add_num_1d)
    from dws_trade_user_cart_add_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度支付最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_payment_nd;
create external table if not exists dws_trade_user_payment_nd
(
    user_id            string         comment '用户 ID',
    payment_count_7d   bigint         comment '最近7日支付次数',
    payment_num_7d     bigint         comment '最近7日支付商品件数',
    payment_amount_7d  decimal(16, 2) comment '最近7日支付金额',
    payment_count_30d  bigint         comment '最近30日支付次数',
    payment_num_30d    bigint         comment '最近30日支付商品件数',
    payment_amount_30d decimal(16, 2) comment '最近30日支付金额'
) comment '交易域用户粒度支付最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_payment_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_payment_nd partition (dt = '2021-08-15')
    select user_id,
           sum(if(dt >= date_add('2021-08-15', -6), payment_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), payment_num_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), payment_amount_1d, 0)),
           sum(payment_count_1d),
           sum(payment_num_1d),
           sum(payment_amount_1d)
    from dws_trade_user_payment_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度退单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_refund_nd;
create external table if not exists dws_trade_user_order_refund_nd
(
    user_id                 string comment '用户 ID',
    order_refund_count_7d   bigint comment '最近7日退单次数',
    order_refund_num_7d     bigint comment '最近7日退单商品件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近7日退单金额',
    order_refund_count_30d  bigint comment '最近30日退单次数',
    order_refund_num_30d    bigint comment '最近30日退单商品件数',
    order_refund_amount_30d decimal(16, 2) comment '最近30日退单金额'
) comment '交易域用户粒度退单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_order_refund_nd' tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_order_refund_nd partition (dt = '2021-08-15')
    select user_id,
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_num_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_refund_amount_1d, 0)),
           sum(order_refund_count_1d),
           sum(order_refund_num_1d),
           sum(order_refund_amount_1d)
    from dws_trade_user_order_refund_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域省份粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_province_order_nd;
create external table if not exists dws_trade_province_order_nd
(
    province_id                string comment '省份 ID',
    province_name              string comment '省份名称',
    area_code                  string comment '地区编码',
    iso_code                   string comment '旧版ISO-3166-2编码',
    iso_3166_2                 string comment '新版ISO-3166-2编码',
    order_count_7d             bigint comment '最近7日下单次数',
    order_original_amount_7d   decimal(16, 2) comment '最近7日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近7日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近7日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近7日下单最终金额',
    order_count_30d            bigint comment '最近30日下单次数',
    order_original_amount_30d  decimal(16, 2) comment '最近30日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近30日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近30日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近30日下单最终金额'
) comment '交易域省份粒度订单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_province_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_province_order_nd partition (dt = '2021-08-15')
    select province_id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2,
           sum(if(dt >= date_add('2021-08-15', -6), order_count_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d, 0)),
           sum(order_count_1d),
           sum(order_original_amount_1d),
           sum(activity_reduce_amount_1d),
           sum(coupon_reduce_amount_1d),
           sum(order_total_amount_1d)
    from dws_trade_province_order_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by province_id, province_name, area_code, iso_code, iso_3166_2;


-- -------------------------------------------------------------------------------------------------
-- 交易域优惠券粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_coupon_order_nd;
create external table if not exists dws_trade_coupon_order_nd
(
    coupon_id                string comment '优惠券 ID',
    coupon_name              string comment '优惠券名称',
    coupon_type_code         string comment '优惠券类型 ID',
    coupon_type_name         string comment '优惠券类型名称',
    coupon_rule              string comment '优惠券规则',
    start_date               string comment '发布日期',
    original_amount_30d      decimal(16, 2) comment '使用下单原始金额',
    coupon_reduce_amount_30d decimal(16, 2) comment '使用下单优惠金额'
) comment '交易域优惠券粒度订单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_coupon_order_nd' tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_coupon_order_nd partition (dt = '2021-08-15')
    select id,
           coupon_name,
           coupon_type_code,
           coupon_type_name,
           benefit_rule,
           start_date,
           sum(split_original_amount),
       sum(split_coupon_amount)
    from 
    (
        select id,
                 coupon_name,
                 coupon_type_code,
                 coupon_type_name,
                 benefit_rule,
                 date_format(start_time, 'yyyy-MM-dd') start_date
        from dim_coupon_full
        where dt = '2021-08-15' and date_format(start_time, 'yyyy-MM-dd') >= date_add('2021-08-15', -29)
    ) cou left join 
    (
        select coupon_id, 
               order_id, 
               split_original_amount, 
               split_coupon_amount
        from dwd_trade_order_detail_inc
        where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15' and coupon_id is not null
    ) od on cou.id = od.coupon_id
    group by id, coupon_name, coupon_type_code, coupon_type_name, benefit_rule, start_date;


-- -------------------------------------------------------------------------------------------------
-- 交易域活动粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_activity_order_nd;
create external table if not exists dws_trade_activity_order_nd
(
    activity_id                string         comment '活动 ID',
    activity_name              string         comment '活动名称',
    activity_type_code         string         comment '活动类型编码',
    activity_type_name         string         comment '活动类型名称',
    start_date                 string         comment '发布日期',
    original_amount_30d        decimal(16, 2) comment '参与活动订单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '参与活动订单优惠金额'
) comment '交易域活动粒度订单最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_activity_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_activity_order_nd partition (dt = '2021-08-15')
    select act.activity_id,
           activity_name,
           activity_type_code,
           activity_type_name,
           date_format(start_time, 'yyyy-MM-dd'),
           sum(split_original_amount),
           sum(split_activity_amount)
    from 
    (
        select activity_id, 
               activity_name, 
               activity_type_code, 
               activity_type_name, 
               start_time
        from dim_activity_full
        where dt = '2021-08-15' and date_format(start_time, 'yyyy-MM-dd') >= date_add('2021-08-15', -29)
        group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
    ) act left join 
    (
        select activity_id, 
               order_id, 
               split_original_amount, 
               split_activity_amount
        from dwd_trade_order_detail_inc
        where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15' and activity_id is not null
    ) od on act.activity_id = od.activity_id
    group by act.activity_id, activity_name, activity_type_code, activity_type_name, start_time;


-- -------------------------------------------------------------------------------------------------
-- 流量域访客页面粒度页面浏览最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_traffic_page_visitor_page_view_nd;
create external table if not exists dws_traffic_page_visitor_page_view_nd
(
    mid_id          string comment '访客 ID',
    brand           string comment '手机品牌',
    model           string comment '手机型号',
    operate_system  string comment '操作系统',
    page_id         string comment '页面 ID',
    during_time_7d  bigint comment '最近7日浏览时长',
    view_count_7d   bigint comment '最近7日访问次数',
    during_time_30d bigint comment '最近30日浏览时长',
    view_count_30d  bigint comment '最近30日访问次数'
) comment '流量域访客页面粒度页面浏览最近n日汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_traffic_page_visitor_page_view_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_traffic_page_visitor_page_view_nd partition (dt = '2021-08-15')
    select mid_id,
           brand,
           model,
           operate_system,
           page_id,
           sum(if(dt >= date_add('2021-08-15', -6), during_time_1d, 0)),
           sum(if(dt >= date_add('2021-08-15', -6), view_count_1d, 0)),
           sum(during_time_1d),
           sum(view_count_1d)
    from dws_traffic_page_visitor_page_view_1d
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
    group by mid_id, brand, model, operate_system, page_id;

-- 最近 N 日汇总表，每日数据装载脚本：dws_1d_to_dws_nd.sh all 2021-08-15



-- -------------------------------------------------------------------------------------------------
-- 历史至今汇总表：交易域用户粒度订单历史至今汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_td;
create external table if not exists dws_trade_user_order_td
(
    user_id                   string         comment '用户 ID',
    order_date_first          string         comment '首次下单日期',
    order_date_last           string         comment '末次下单日期',
    order_count_td            bigint         comment '历史至今下单次数',
    order_num_td              bigint         comment '历史至今购买商品件数',
    original_amount_td        decimal(16, 2) comment '历史至今原始金额',
    activity_reduce_amount_td decimal(16, 2) comment '历史至今活动优惠金额',
    coupon_reduce_amount_td   decimal(16, 2) comment '历史至今优惠券优惠金额',
    total_amount_td           decimal(16, 2) comment '历史至今最终金额'
) comment '交易域用户粒度订单历史至今汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_order_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2021-08-15')
    select user_id,
           min(dt)                        order_date_first,
           max(dt)                        order_date_last,
           sum(order_count_1d)            order_count,
           sum(order_num_1d)              order_num,
           sum(order_original_amount_1d)  original_amount,
           sum(activity_reduce_amount_1d) activity_reduce_amount,
           sum(coupon_reduce_amount_1d)   coupon_reduce_amount,
           sum(order_total_amount_1d)     total_amount
    from dws_trade_user_order_1d
    group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2020-06-15')
    select nvl(old.user_id, new.user_id),
           if(old.user_id is null, '2020-06-15', old.order_date_first),
           if(new.user_id is not null, '2020-06-15', old.order_date_last),
           nvl(old.order_count_td, 0) + nvl(new.order_count_1d, 0),
           nvl(old.order_num_td, 0) + nvl(new.order_num_1d, 0),
           nvl(old.original_amount_td, 0) + nvl(new.order_original_amount_1d, 0),
           nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0),
           nvl(old.coupon_reduce_amount_td, 0) + nvl(new.coupon_reduce_amount_1d, 0),
           nvl(old.total_amount_td, 0) + nvl(new.order_total_amount_1d, 0)
    from 
    (
        select user_id,
                 order_date_first,
                 order_date_last,
                 order_count_td,
                 order_num_td,
                 original_amount_td,
                 activity_reduce_amount_td,
                 coupon_reduce_amount_td,
                 total_amount_td
        from dws_trade_user_order_td
        where dt = date_add('2020-06-15', -1)
    ) old full outer join 
    (
        select user_id,
               order_count_1d,
               order_num_1d,
               order_original_amount_1d,
               activity_reduce_amount_1d,
               coupon_reduce_amount_1d,
               order_total_amount_1d
        from dws_trade_user_order_1d
        where dt = '2020-06-15'
    ) new on old.user_id = new.user_id;


-- -------------------------------------------------------------------------------------------------
-- 装载数据交易域用户粒度支付历史至今汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_payment_td;
create external table if not exists dws_trade_user_payment_td
(
    user_id            string         comment '用户 ID',
    payment_date_first string         comment '首次支付日期',
    payment_date_last  string         comment '末次支付日期',
    payment_count_td   bigint         comment '历史至今支付次数',
    payment_num_td     bigint         comment '历史至今支付商品件数',
    payment_amount_td  decimal(16, 2) comment '历史至今支付金额'
) comment '交易域用户粒度支付历史至今汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_trade_user_payment_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2021-08-15')
    select user_id,
           min(dt)                payment_date_first,
           max(dt)                payment_date_last,
           sum(payment_count_1d)  payment_count,
           sum(payment_num_1d)    payment_num,
           sum(payment_amount_1d) payment_amount
    from dws_trade_user_payment_1d
    group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2020-06-15')
    select nvl(old.user_id, new.user_id),
           if(old.user_id is null, '2020-06-15', old.payment_date_first),
           if(new.user_id is not null, '2020-06-15', old.payment_date_last),
           nvl(old.payment_count_td, 0) + nvl(new.payment_count_1d, 0),
           nvl(old.payment_num_td, 0) + nvl(new.payment_num_1d, 0),
           nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)
    from 
    (
        select user_id, 
               payment_date_first, 
               payment_date_last, 
               payment_count_td, 
               payment_num_td, 
               payment_amount_td
        from dws_trade_user_payment_td
        where dt = date_add('2020-06-15', -1)
    ) old full outer join 
    (
        select user_id, 
               payment_count_1d, 
               payment_num_1d, 
               payment_amount_1d
       from dws_trade_user_payment_1d
       where dt = '2020-06-15'
    ) new on old.user_id = new.user_id;

-- -------------------------------------------------------------------------------------------------
-- 用户域用户粒度登录历史至今汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_user_user_login_td;
create external table if not exists dws_user_user_login_td
(
    user_id         string comment '用户 ID', 
    login_date_last string comment '末次登录日期', 
    login_count_td  bigint comment '累计登录次数'
) comment '用户域用户粒度登录历史至今汇总事实表' partitioned by (dt string) 
    stored as orc location '/warehouse/dws/dws_user_user_login_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_user_user_login_td partition (dt = '2021-08-15')
    select u.id, 
           nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')), 
           nvl(login_count_td, 1)
    from 
    (
        select id, create_time from dim_user_zip where dt = '9999-12-31'
    ) u left join 
    (
        select user_id, 
               max(dt) login_date_last, 
               count(*) login_count_td
        from dwd_user_login_inc
        group by user_id
    ) l on u.id = l.user_id;

-- 每日装载
insert overwrite table dws_user_user_login_td partition (dt = '2020-06-15')
select nvl(old.user_id, new.user_id),
       if(new.user_id is null, old.login_date_last, '2020-06-15'),
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)
    from 
    (
        select user_id, 
               login_date_last, 
               login_count_td
        from dws_user_user_login_td
        where dt = date_add('2020-06-15', -1)
    ) old full outer join 
    (
        select user_id, 
               count(*) login_count_1d
        from dwd_user_login_inc
        where dt = '2020-06-15'
        group by user_id
    ) new on old.user_id = new.user_id;


-- -------------------------------------------------------------------------------------------------
-- 历史至今汇总表，首日数据装载脚本：dws_1d_to_dws_td_init.sh all 2021-08-15
-- 历史至今汇总表，每日数据装载脚本：dws_1d_to_dws_td.sh all 2021-08-15

-- -------------------------------------------------------------------------------------------------
-- DWS 层设计要点：
--         DWS 层的设计参考指标体系
--         DWS 层的数据存储格式为 orc 列式存储 + snappy 压缩。
--         DWS 层表名的命名规范为 dws_数据域_统计粒度_业务过程_统计周期（1d/nd/td）
-- -------------------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度订单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_1d;
create external table if not exists dws_trade_user_sku_order_1d
(
    user_id                   string         comment '用户 ID',
    sku_id                    string         comment 'SKU ID',
    sku_name                  string         comment 'SKU 名称',
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
) comment '交易域用户商品粒度订单最近 1 日汇总事实表' 
    partitioned by (dt string)
    stored as orc 
    location '/warehouse/dws/dws_trade_user_sku_order_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_1d partition (dt)
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
    from dwd_trade_order_detail_inc
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
   from dim_sku_full
   where dt = '2021-08-15'
) as sku on order_detail.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_1d partition (dt = '2021-08-15')
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
       order_detail.order_total_amount_1d
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
    from dwd_trade_order_detail_inc
    where dt='2021-08-16'
    group by user_id, sku_id
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
   from dim_sku_full
   where dt = '2021-08-16'
) as sku on order_detail.sku_id = sku.id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度退单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_refund_1d;
create external table if not exists dws_trade_user_sku_order_refund_1d
(
    user_id                string         comment '用户 ID',
    sku_id                 string         comment 'SKU ID',
    sku_name               string         comment 'SKU 名称',
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
) comment '交易域用户商品粒度退单最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_sku_order_refund_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt)
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
    from dwd_trade_order_refund_inc
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
    from dim_sku_full
    where dt = '2021-08-15'
) as sku on order_refund.sku_id = sku.id;

-- 每日装载
insert overwrite table dws_trade_user_sku_order_refund_1d partition (dt = '2021-08-16')
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
       order_refund.order_refund_amount
from 
(
    select dt,
           user_id,
           sku_id,
           count(*)           as order_refund_count,
           sum(refund_num)    as order_refund_num,
           sum(refund_amount) as order_refund_amount
    from dwd_trade_order_refund_inc
    where dt='2021-08-16'
    group by user_id, sku_id
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
    from dim_sku_full
    where dt = '2021-08-16'
) as sku on order_refund.sku_id = sku.id;


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
) comment '交易域用户粒度订单最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_order_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_1d partition (dt)
select user_id,
       count(distinct (order_id))         as order_count_1d,
       sum(sku_num)                       as order_num_1d,
       sum(split_original_amount)         as order_original_amount_1d,
       sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,
       sum(nvl(split_coupon_amount,   0)) as coupon_reduce_amount_1d,
       sum(split_total_amount)            as order_total_amount_1d,
       dt
from dwd_trade_order_detail_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_1d partition (dt = '2020-08-16')
select user_id,
       count(distinct (order_id))         as order_count_1d,
       sum(sku_num)                       as order_num_1d,
       sum(split_original_amount)         as order_original_amount_1d,
       sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,
       sum(nvl(split_coupon_amount,   0)) as coupon_reduce_amount_1d,
       sum(split_total_amount)            as order_total_amount_1d
from dwd_trade_order_detail_inc
where dt = '2020-08-16'
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
) comment '交易域用户粒度加购最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_cart_add_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt)
select user_id, 
       count(*)      as cart_add_count_1d, 
       sum(sku_num)  as cart_add_num_1d,  
       dt
from dwd_trade_cart_add_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_cart_add_1d partition (dt = '2021-08-16')
select user_id, 
       count(*)      as cart_add_count_1d, 
       sum(sku_num)  as cart_add_num_1d
from dwd_trade_cart_add_inc
where dt = '2021-08-16'
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
) comment '交易域用户粒度支付最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_payment_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_1d partition (dt)
select user_id, 
       count(distinct (order_id)) as payment_count_1d, 
       sum(sku_num)               as payment_num_1d, 
       sum(split_payment_amount)  as payment_amount_1d, 
       dt
from dwd_trade_pay_detail_suc_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_payment_1d partition (dt = '2021-08-16')
select user_id,
       count(distinct (order_id)) as payment_count_1d, 
       sum(sku_num)               as payment_num_1d, 
       sum(split_payment_amount)  as payment_amount_1d
from dwd_trade_pay_detail_suc_inc
where dt = '2021-08-16'
group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 最近 1 交易域用户粒度退单最近 1 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_refund_1d;
create external table if not exists dws_trade_user_order_refund_1d
(
    user_id                string         comment '用户 ID',
    order_refund_count_1d  bigint         comment '最近 1 日退单次数',
    order_refund_num_1d    bigint         comment '最近 1 日退单商品件数',
    order_refund_amount_1d decimal(16, 2) comment '最近 1 日退单金额'
) comment '交易域用户粒度退单最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_order_refund_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_user_order_refund_1d partition (dt)
select user_id,
       count(*)           as order_refund_count,
       sum(refund_num)    as order_refund_num,
       sum(refund_amount) as order_refund_amount,
       dt
from dwd_trade_order_refund_inc
group by user_id, dt;

-- 每日装载
insert overwrite table dws_trade_user_order_refund_1d partition (dt = '2021-08-16')
select user_id, 
       count(*)           as order_refund_count,
       sum(refund_num)    as order_refund_num,
       sum(refund_amount) as order_refund_amount
from dwd_trade_order_refund_inc
where dt = '2021-08-16'
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
    iso_code                  string         comment '旧版 ISO-3166-2 编码',
    iso_3166_2                string         comment '新版版 ISO-3166-2 编码',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近 1 日下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域省份粒度订单最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_province_order_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_province_order_1d partition (dt)
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
    from dwd_trade_order_detail_inc
    group by province_id, dt
) as order_detail left join 
(
    select id, 
           province_name, 
           area_code, 
           iso_code, 
           iso_3166_2
    from dim_province_full
    where dt = '2021-08-15'
) as province on order_detail.province_id = province.id;

-- 每日装载
insert overwrite table dws_trade_province_order_1d partition (dt = '2021-08-16')
select province.id                             as province_id,
       province.province_name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       order_detail.order_count_1d,
       order_detail.order_original_amount_1d,
       order_detail.activity_reduce_amount_1d,
       order_detail.coupon_reduce_amount_1d,
       order_detail.order_total_amount_1d
from 
(
    select province_id,
           count(distinct (order_id))         as order_count_1d,
           sum(split_original_amount)         as order_original_amount_1d,
           sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,
           sum(nvl(split_coupon_amount, 0))   as coupon_reduce_amount_1d,
           sum(split_total_amount)            as order_total_amount_1d,
           dt
    from dwd_trade_order_detail_inc
    where dt = '2021-08-16'
    group by province_id
) as order_detail left join 
(
    select id, 
           province_name, 
           area_code, 
           iso_code, 
           iso_3166_2
    from dim_province_full
    where dt = '2021-08-16'
) as province on order_detail.province_id = province.id;


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
    version_code   string comment 'APP 版本号',
    channel        string comment '渠道',
    during_time_1d bigint comment '最近 1 日访问时长',
    page_count_1d  bigint comment '最近 1 日访问页面数'
) comment '流量域会话粒度页面浏览最近 1 日汇总表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_traffic_session_page_view_1d' 
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
       sum(during_time) as during_time_1d,
       count(*)         as page_count_1d
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
) comment '流量域访客页面粒度页面浏览最近 1 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_traffic_page_visitor_page_view_1d' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dws_traffic_page_visitor_page_view_1d partition (dt = '2021-08-15')
select mid_id, 
       brand, 
       model, 
       operate_system, 
       page_id, 
       sum(during_time) as during_time_1d, 
       count(*)         as view_count_1d
from dwd_traffic_page_view_inc
where dt = '2021-08-15'
group by mid_id, brand, model, operate_system, page_id;


-- -------------------------------------------------------------------------------------------------
-- 最近 1 日汇总表，首日数据装载脚本：dwd-dws-1d-init.sh all 2021-08-15
-- 最近 1 日汇总表，每日数据装载脚本：dwd-dws_1d.sh      all 2021-08-15
-- -------------------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------------------
-- 最近 N 日汇总表：交易域用户商品粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_nd;
create external table if not exists dws_trade_user_sku_order_nd
(
    user_id                    string         comment '用户 ID',
    sku_id                     string         comment 'SKU ID',
    sku_name                   string         comment 'SKU 名称',
    category1_id               string         comment '一级分类 ID',
    category1_name             string         comment '一级分类名称',
    category2_id               string         comment '一级分类 ID',
    category2_name             string         comment '一级分类名称',
    category3_id               string         comment '一级分类 ID',
    category3_name             string         comment '一级分类名称',
    tm_id                      string         comment '品牌 ID',
    tm_name                    string         comment '品牌名称',
    order_count_7d             string         comment '最近 7 日下单次数',
    order_num_7d               bigint         comment '最近 7 日下单件数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_num_30d              bigint         comment '最近 30 日下单件数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域用户商品粒度订单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_sku_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
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
       sum(if(dt >= date_add('2021-08-15', -6), order_count_1d,            0)) as order_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_num_1d,              0)) as order_num_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
       sum(order_count_1d)                                                     as order_count_30d,
       sum(order_num_1d)                                                       as order_num_30d,
       sum(order_original_amount_1d)                                           as order_original_amount_30d,
       sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,
       sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
       sum(order_total_amount_1d)                                              as order_total_amount_30d    
from dws_trade_user_sku_order_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,
         category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度退单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_sku_order_refund_nd;
create external table if not exists dws_trade_user_sku_order_refund_nd
(
    user_id                 string         comment '用户 ID',
    sku_id                  string         comment 'SKU ID',
    sku_name                string         comment 'SKU 名称',
    category1_id            string         comment '一级分类 ID',
    category1_name          string         comment '一级分类名称',
    category2_id            string         comment '一级分类 ID',
    category2_name          string         comment '一级分类名称',
    category3_id            string         comment '一级分类 ID',
    category3_name          string         comment '一级分类名称',
    tm_id                   string         comment '品牌 ID',
    tm_name                 string         comment '品牌名称',
    order_refund_count_7d   bigint         comment '最近 7 日退单次数',
    order_refund_num_7d     bigint         comment '最近 7 日退单件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近 7 日退单金额',
    order_refund_count_30d  bigint         comment '最近 30 日退单次数',
    order_refund_num_30d    bigint         comment '最近 30 日退单件数',
    order_refund_amount_30d decimal(16, 2) comment '最近 30 日退单金额'
) comment '交易域用户商品粒度退单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_sku_order_refund_nd' 
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
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_count_1d,  0)) as order_refund_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_num_1d,    0)) as order_refund_num_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,
       sum(order_refund_count_1d)                                           as order_refund_count_30d,
       sum(order_refund_num_1d)                                             as order_refund_num_30d,
       sum(order_refund_amount_1d)                                          as order_refund_amount_30d
from dws_trade_user_sku_order_refund_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,
         category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_nd;
create external table if not exists dws_trade_user_order_nd
(
    user_id                    string         comment '用户 ID',
    order_count_7d             bigint         comment '最近 7 日下单次数',
    order_num_7d               bigint         comment '最近 7 日下单商品件数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_num_30d              bigint         comment '最近 30 日下单商品件数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域用户粒度订单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_order_nd partition (dt = '2021-08-15')
    select user_id,
           sum(if(dt >= date_add('2021-08-15', -6), order_count_1d,            0)) as order_count_7d,
           sum(if(dt >= date_add('2021-08-15', -6), order_num_1d,              0)) as order_num_7d,
           sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
           sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
           sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
           sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
           sum(order_count_1d)                                                     as order_count_30d,
           sum(order_num_1d)                                                       as order_num_30d,
           sum(order_original_amount_1d)                                           as order_original_amount_30d,
           sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,
           sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
           sum(order_total_amount_1d)                                              as order_total_amount_30d
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
    cart_add_count_7d  bigint comment '最近 7 日加购次数',
    cart_add_num_7d    bigint comment '最近 7 日加购商品件数',
    cart_add_count_30d bigint comment '最近 30 日加购次数',
    cart_add_num_30d   bigint comment '最近 30 日加购商品件数'
) comment '交易域用户粒度加购最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_cart_add_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_cart_add_nd partition (dt = '2021-08-15')
select user_id,
       sum(if(dt >= date_add('2021-08-15', -6), cart_add_count_1d, 0)) as cart_add_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), cart_add_num_1d,   0)) as cart_add_num_7d,
       sum(cart_add_count_1d)                                          as cart_add_count_30d,
       sum(cart_add_num_1d)                                            as cart_add_num_30d
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
    payment_count_7d   bigint         comment '最近 7 日支付次数',
    payment_num_7d     bigint         comment '最近 7 日支付商品件数',
    payment_amount_7d  decimal(16, 2) comment '最近 7 日支付金额',
    payment_count_30d  bigint         comment '最近 30 日支付次数',
    payment_num_30d    bigint         comment '最近 30 日支付商品件数',
    payment_amount_30d decimal(16, 2) comment '最近 30 日支付金额'
) comment '交易域用户粒度支付最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_payment_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_payment_nd partition (dt = '2021-08-15')
select user_id,
       sum(if(dt >= date_add('2021-08-15', -6), payment_count_1d,  0)) as payment_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), payment_num_1d,    0)) as payment_num_7d,
       sum(if(dt >= date_add('2021-08-15', -6), payment_amount_1d, 0)) as payment_amount_7d,
       sum(payment_count_1d)                                           as payment_count_30d,
       sum(payment_num_1d)                                             as payment_num_30d,
       sum(payment_amount_1d)                                          as payment_amount_30d
from dws_trade_user_payment_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域用户粒度退单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_user_order_refund_nd;
create external table if not exists dws_trade_user_order_refund_nd
(
    user_id                 string         comment '用户 ID',
    order_refund_count_7d   bigint         comment '最近 7 日退单次数',
    order_refund_num_7d     bigint         comment '最近 7 日退单商品件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近 7 日退单金额',
    order_refund_count_30d  bigint         comment '最近 30 日退单次数',
    order_refund_num_30d    bigint         comment '最近 30 日退单商品件数',
    order_refund_amount_30d decimal(16, 2) comment '最近 30 日退单金额'
) comment '交易域用户粒度退单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_order_refund_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_user_order_refund_nd partition (dt = '2021-08-15')
select user_id,
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_count_1d,  0)) as order_refund_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_num_1d,    0)) as order_refund_num_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,
       sum(order_refund_count_1d)                                           as order_refund_count_30d,
       sum(order_refund_num_1d)                                             as order_refund_num_30d,
       sum(order_refund_amount_1d)                                          as order_refund_amount_30d
from dws_trade_user_order_refund_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by user_id;


-- -------------------------------------------------------------------------------------------------
-- 交易域省份粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_province_order_nd;
create external table if not exists dws_trade_province_order_nd
(
    province_id                string         comment '省份 ID',
    province_name              string         comment '省份名称',
    area_code                  string         comment '地区编码',
    iso_code                   string         comment '旧版 ISO-3166-2 编码',
    iso_3166_2                 string         comment '新版 ISO-3166-2 编码',
    order_count_7d             bigint         comment '最近 7 日下单次数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域省份粒度订单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_province_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_province_order_nd partition (dt = '2021-08-15')
select province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(if(dt >= date_add('2021-08-15', -6), order_count_1d,            0)) as order_count_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_original_amount_1d,  0)) as order_original_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,
       sum(if(dt >= date_add('2021-08-15', -6), order_total_amount_1d,     0)) as order_total_amount_7d,
       sum(order_count_1d)                                                     as order_count_30d,
       sum(order_original_amount_1d)                                           as order_original_amount_30d,
       sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30,
       sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,
       sum(order_total_amount_1d)                                              as order_total_amount_30d
from dws_trade_province_order_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by province_id, province_name, area_code, iso_code, iso_3166_2;


-- -------------------------------------------------------------------------------------------------
-- 交易域优惠券粒度订单最近 N 日汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_trade_coupon_order_nd;
create external table if not exists dws_trade_coupon_order_nd
(
    coupon_id                string         comment '优惠券 ID',
    coupon_name              string         comment '优惠券名称',
    coupon_type_code         string         comment '优惠券类型 ID',
    coupon_type_name         string         comment '优惠券类型名称',
    coupon_rule              string         comment '优惠券规则',
    start_date               string         comment '发布日期',
    original_amount_30d      decimal(16, 2) comment '使用下单原始金额',
    coupon_reduce_amount_30d decimal(16, 2) comment '使用下单优惠金额'
) comment '交易域优惠券粒度订单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_coupon_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_coupon_order_nd partition (dt = '2021-08-15')
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
    from dim_coupon_full
    where dt = '2021-08-15' and date_format(start_time, 'yyyy-MM-dd') >= date_add('2021-08-15', -29)
) as coupon left join 
(
    select coupon_id, 
           order_id, 
           split_original_amount, 
           split_coupon_amount
    from dwd_trade_order_detail_inc
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15' and coupon_id is not null
) as order_detail 
    on coupon.id = order_detail.coupon_id
group by coupon.id,               coupon.coupon_name,  coupon.coupon_type_code,
         coupon.coupon_type_name, coupon.benefit_rule, coupon.start_date;


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
) comment '交易域活动粒度订单最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_activity_order_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_trade_activity_order_nd partition (dt = '2021-08-15')
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
    from dim_activity_full
    where dt = '2021-08-15' and date_format(start_time, 'yyyy-MM-dd') >= date_add('2021-08-15', -29)
    group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
) as activity left join 
(
    select activity_id, 
           order_id, 
           split_original_amount, 
           split_activity_amount
    from dwd_trade_order_detail_inc
    where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15' and activity_id is not null
) as order_detail on activity.activity_id = order_detail.activity_id
group by activity.activity_id,        activity.activity_name, activity.activity_type_code,
         activity.activity_type_name, activity.start_time;


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
    during_time_7d  bigint comment '最近 7 日浏览时长',
    view_count_7d   bigint comment '最近 7 日访问次数',
    during_time_30d bigint comment '最近 30 日浏览时长',
    view_count_30d  bigint comment '最近 30 日访问次数'
) comment '流量域访客页面粒度页面浏览最近 N 日汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_traffic_page_visitor_page_view_nd' 
    tblproperties ('orc.compress' = 'snappy');

-- 装载数据
insert overwrite table dws_traffic_page_visitor_page_view_nd partition (dt = '2021-08-15')
select mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(if(dt >= date_add('2021-08-15', -6), during_time_1d, 0)) as during_time_7d,
       sum(if(dt >= date_add('2021-08-15', -6), view_count_1d,  0)) as view_count_7d,
       sum(during_time_1d)                                          as during_time_30d,
       sum(view_count_1d)                                           as view_count_30d
from dws_traffic_page_visitor_page_view_1d
where dt >= date_add('2021-08-15', -29) and dt <= '2021-08-15'
group by mid_id, brand, model, operate_system, page_id;


-- -------------------------------------------------------------------------------------------------
-- 最近 N 日汇总表，每日数据装载脚本：dwd-dws-nd.sh all 2021-08-15
-- -------------------------------------------------------------------------------------------------


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
) comment '交易域用户粒度订单历史至今汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_order_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2021-08-15')
select user_id,
       min(dt)                        as order_date_first,
       max(dt)                        as order_date_last,
       sum(order_count_1d)            as order_count_td,
       sum(order_num_1d)              as order_num_td,
       sum(order_original_amount_1d)  as original_amount_td,
       sum(activity_reduce_amount_1d) as activity_reduce_amount_td,
       sum(coupon_reduce_amount_1d)   as coupon_reduce_amount_td,
       sum(order_total_amount_1d)     as total_amount_td
from dws_trade_user_order_1d
group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_order_td partition (dt = '2021-08-16')
select nvl(old.user_id, new.user_id)                                                 as user_id,
       if(old.user_id is null,     '2021-08-15', old.order_date_first)               as order_date_first,
       if(new.user_id is not null, '2021-08-15', old.order_date_last)                as order_date_last,
       nvl(old.order_count_td,            0) + nvl(new.order_count_1d,            0) as order_count_td,
       nvl(old.order_num_td,              0) + nvl(new.order_num_1d,              0) as order_num_td,
       nvl(old.original_amount_td,        0) + nvl(new.order_original_amount_1d,  0) as original_amount_td,
       nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0) as activity_reduce_amount_td,
       nvl(old.coupon_reduce_amount_td,   0) + nvl(new.coupon_reduce_amount_1d,   0) as coupon_reduce_amount_td,
       nvl(old.total_amount_td,           0) + nvl(new.order_total_amount_1d,     0) as total_amount_td
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
    where dt = date_add('2021-08-16', -1)
) as old full outer join 
(
    select user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d
    from dws_trade_user_order_1d
    where dt = '2021-08-16'
) as new on old.user_id = new.user_id;


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
) comment '交易域用户粒度支付历史至今汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_trade_user_payment_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2021-08-15')
select user_id,
       min(dt)                as payment_date_first,
       max(dt)                as payment_date_last,
       sum(payment_count_1d)  as payment_count_td,
       sum(payment_num_1d)    as payment_num_td,
       sum(payment_amount_1d) as payment_amount_td
from dws_trade_user_payment_1d
group by user_id;

-- 每日装载
insert overwrite table dws_trade_user_payment_td partition (dt = '2021-08-16')
select nvl(old.user_id, new.user_id)                                     as user_id,
       if(old.user_id is null,     '2021-08-15', old.payment_date_first) as payment_date_first,
       if(new.user_id is not null, '2021-08-15', old.payment_date_last)  as payment_date_last,
       nvl(old.payment_count_td,  0) + nvl(new.payment_count_1d, 0)      as payment_count_td,
       nvl(old.payment_num_td,    0) + nvl(new.payment_num_1d, 0)        as payment_num_td,
       nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)     as payment_amount_td
from 
(
    select user_id, 
           payment_date_first, 
           payment_date_last, 
           payment_count_td, 
           payment_num_td, 
           payment_amount_td
    from dws_trade_user_payment_td
    where dt = date_add('2021-08-16', -1)
) as old full outer join 
(
    select user_id, 
           payment_count_1d, 
           payment_num_1d, 
           payment_amount_1d
   from dws_trade_user_payment_1d
   where dt = '2021-08-16'
) as new on old.user_id = new.user_id;

-- -------------------------------------------------------------------------------------------------
-- 用户域用户粒度登录历史至今汇总表
-- -------------------------------------------------------------------------------------------------
drop table if exists dws_user_user_login_td;
create external table if not exists dws_user_user_login_td
(
    user_id         string comment '用户 ID', 
    login_date_last string comment '末次登录日期',
    login_count_td  bigint comment '累计登录次数'
) comment '用户域用户粒度登录历史至今汇总事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dws/dws_user_user_login_td' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dws_user_user_login_td partition (dt = '2021-08-15')
select user_.id                                                                 as user_id, 
       nvl(login.login_date_last, date_format(user_.create_time, 'yyyy-MM-dd')) as login_date_last, 
       nvl(login.login_count_td,  1)                                            as login_count_td
from 
(
    select id, 
           create_time 
    from dim_user_zip 
    where dt = '9999-12-31'
) as user_ left join 
(
    select user_id, 
           max(dt) login_date_last, 
           count(*) login_count_td
    from dwd_user_login_inc
    group by user_id
) as login on user_.id = login.user_id;

-- 每日装载
insert overwrite table dws_user_user_login_td partition (dt = '2021-08-16')
select nvl(old.user_id, new.user_id)                              as user_id,
       if(new.user_id is null, old.login_date_last, '2021-08-15') as login_date_last,
       nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)    as login_count_td
from 
(
    select user_id, 
           login_date_last, 
           login_count_td
    from dws_user_user_login_td
    where dt = date_add('2021-08-16', -1)
) as old full outer join 
(
    select user_id, 
           count(*) as login_count_1d
    from dwd_user_login_inc
    where dt = '2021-08-16'
    group by user_id
) as new on old.user_id = new.user_id;


-- -------------------------------------------------------------------------------------------------
-- 历史至今汇总表，首日数据装载脚本：dwd-dws-td-init.sh all 2021-08-15
-- 历史至今汇总表，每日数据装载脚本：dwd-dws-td.sh      all 2021-08-15
-- -------------------------------------------------------------------------------------------------

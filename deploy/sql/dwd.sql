-- -------------------------------------------------------------------------------------------------
-- 1. DWD 层的设计依据是维度建模理论，该层存储维度模型的事实表
-- 2. DWD 层的数据存储格式为 orc 列式存储 + snappy 压缩
-- 3. DWD 层表名的命名规范为 dwd_数据域_表名_单分区增量全量标识（inc/full）
-- -------------------------------------------------------------------------------------------------

-- -------------------------------------------------------------------------------------------------
-- 交易域加购事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_cart_add_inc;
create external table if not exists dwd_trade_cart_add_inc
(
    id               string comment '编号',
    user_id          string comment '用户 ID',
    sku_id           string comment '商品 ID',
    date_id          string comment '时间 ID',
    create_time      string comment '加购时间',
    source_id        string comment '来源类型 ID',
    source_type_code string comment '来源类型编码',
    source_type_name string comment '来源类型名称',
    sku_num          bigint comment '加购物车件数'
) comment '交易域加购物车事务事实表' 
    partitioned by (`dt` string)
    row format delimited fields terminated by '\t' 
    stored as orc
    location '/warehouse/dwd/dwd_trade_cart_add_inc/' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select cart.id,
       cart.user_id,
       cart.sku_id,
       date_format(cart.create_time, 'yyyy-MM-dd') as date_id,
       cart.create_time,
       cart.source_id,
       cart.source_type                            as source_type_code,
       dic.dic_name                                as source_type_name,
       cart.sku_num,
       date_format(cart.create_time, 'yyyy-MM-dd') as dt
from 
(   
    select data.id, 
           data.user_id, 
           data.sku_id, 
           data.create_time, 
           data.source_id, 
           data.source_type, 
           data.sku_num
    from ods_cart_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as cart left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '24'
) as dic on cart.source_type = dic.dic_code;

-- 每日数据装载
insert overwrite table dwd_trade_cart_add_inc partition (dt = '2021-08-16')
select cart.id,
       cart.user_id,
       cart.sku_id,
       cart.date_id,
       cart.create_time,
       cart.source_id,
       cart.source_type                            as source_type_code,
       dic.dic_name                                as source_type_name,
       cart.sku_num
from 
(   
    select data.id, 
           data.user_id, 
           data.sku_id,
           date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyyMM-dd')          as date_id,
           date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyyMM-dd HH:mm:ss') as create_time,
           data.source_id, 
           data.source_type,
           if(type = 'insert', data.sku_num, data.sku_num-old['sku_num'])             as sku_num
    from ods_cart_info_inc
    where   dt    = '2021-08-16' 
        and (type = 'insert' or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as int)))
) as cart left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '24'
) as dic on cart.source_type = dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域下单事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_order_detail_inc;
create external table if not exists dwd_trade_order_detail_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    date_id               string         comment '下单日期 ID',
    create_time           string         comment '下单时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '原始价格',
    split_activity_amount decimal(16, 2) comment '活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '优惠券优惠分摊',
    split_total_amount    decimal(16, 2) comment '最终价格分摊'
) comment '交易域下单明细事务事实表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_order_detail_inc/' 
    tblproperties ('orc.compress' = 'snappy');


-- 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (dt)
select detail.id,
       detail.order_id,
       info.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       date_format(detail.create_time, 'yyyy-MM-dd')  as date_id,
       detail.create_time,
       detail.source_id,
       detail.source_type                             as source_type_code,
       dic.dic_name                                   as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount,
       date_format(detail.create_time, 'yyyy-MM-dd')  as dt
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.create_time,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price as split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as detail left join 
(
    select data.id, 
           data.user_id, 
           data.province_id
	from ods_order_info_inc
	where dt = '2021-08-15' and type = 'bootstrap-insert'
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '24'
) as dic on detail.source_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_order_detail_inc partition (dt = '2021-08-16')
select detail.id,
       detail.order_id,
       info.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       date_format(detail.create_time, 'yyyy-MM-dd')  as date_id,
       detail.create_time,
       detail.source_id,
       detail.source_type                             as source_type_code,
       dic.dic_name                                   as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.create_time,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price as split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2021-08-16' and type = 'insert'
) as detail left join 
(
    select data.id, 
           data.user_id, 
           data.province_id
	from ods_order_info_inc
	where dt = '2021-08-16' and type = 'insert'
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2021-08-15' and type = 'insert'
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2021-08-15' and type = 'insert'
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '24'
) as dic on detail.source_type = dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域取消订单事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_cancel_detail_inc;
create external table if not exists dwd_trade_cancel_detail_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    date_id               string         comment '取消订单日期 ID',
    cancel_time           string         comment '取消订单时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '原始价格',
    split_activity_amount decimal(16, 2) comment '活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '优惠券优惠分摊',
    split_total_amount    decimal(16, 2) comment '最终价格分摊'
) comment '交易域取消订单明细事务事实表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_cancel_detail_inc/' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cancel_detail_inc partition (dt)
select detail.id,
       detail.order_id,
       info.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       date_format(info.canel_time, 'yyyy-MM-dd') as date_id,
       info.canel_time,
       detail.source_id ,
       detail.source_type                         as source_type_code,
       dic.dic_name                               as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount,
       date_format(info.canel_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price as split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc 
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as detail join 
(
    select data.id, 
           data.user_id, 
           data.province_id, 
           data.operate_time  as canel_time
	from ods_order_info_inc
	where dt = '2021-08-15' and type = 'bootstrap-insert' and data.order_status = '1003'
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
	from ods_order_detail_activity_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '24'
) as dic on detail.source_type = dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_cancel_detail_inc partition (dt = '2021-08-16')
select detail.id,
       detail.order_id,
       info.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       date_format(info.canel_time, 'yyyy-MM-dd') as date_id,
       info.canel_time,
       detail.source_id ,
       detail.source_type                         as source_type_code,
       dic.dic_name                               as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc 
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as detail join 
(
    select data.id, 
           data.user_id, 
           data.province_id, 
           data.operate_time  as canel_time
	from ods_order_info_inc
    where   dt = '2021-08-16' and type = 'update' and data.order_status = '1003' 
        and array_contains(map_keys(old), 'order_status')
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
	from ods_order_detail_activity_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '24'
) as dic on detail.source_type = dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域支付成功事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_pay_detail_suc_inc;
create external table if not exists dwd_trade_pay_detail_suc_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动规则 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    payment_type_code     string         comment '支付类型编码',
    payment_type_name     string         comment '支付类型名称',
    date_id               string         comment '支付日期 ID',
    callback_time         string         comment '支付成功时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '应支付原始金额',
    split_activity_amount decimal(16, 2) comment '支付活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '支付优惠券优惠分摊',
    split_payment_amount  decimal(16, 2) comment '支付金额'
) comment '交易域成功支付事务事实表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_pay_detail_suc_inc/' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日数据装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select detail.id,
       detail.order_id,
       payment.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       payment.payment_type                             as payment_type_code,
       pay_dic.dic_name                                 as payment_type_name,
       date_format(payment.callback_time, 'yyyy-MM-dd') as date_id,
       payment.callback_time,
       detail.source_id,
       detail.source_type                               as source_type_code,
       src_dic.dic_name                                 as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount,
       date_format(payment.callback_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price as split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as detail join 
(
    select data.user_id, 
           data.order_id, 
           data.payment_type, 
           data.callback_time
    from ods_payment_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert' and data.payment_status = '1602'
) as payment 
    on detail.order_id = payment.order_id 
left join 
(
    select data.id, 
           data.province_id
    from ods_order_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
    from ods_order_detail_activity_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '11'
) as pay_dic 
    on payment.payment_type = pay_dic.dic_code 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '24'
) as src_dic on detail.source_type = src_dic.dic_code;

-- 每日装载
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt = '2021-08-16')
select detail.id,
       detail.order_id,
       payment.user_id,
       detail.sku_id,
       info.province_id,
       activity.activity_id,
       activity.activity_rule_id,
       coupon.coupon_id,
       payment.payment_type                             as payment_type_code,
       pay_dic.dic_name                                 as payment_type_name,
       date_format(payment.callback_time, 'yyyy-MM-dd') as date_id,
       payment.callback_time,
       detail.source_id,
       detail.source_type                               as source_type_code,
       src_dic.dic_name                                 as source_type_name,
       detail.sku_num,
       detail.split_original_amount,
       detail.split_activity_amount,
       detail.split_coupon_amount,
       detail.split_total_amount
from 
(
    select data.id,
           data.order_id,
           data.sku_id,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price as split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
    from ods_order_detail_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as detail join 
(
    select data.user_id, 
           data.order_id, 
           data.payment_type, 
           data.callback_time
    from ods_payment_info_inc
    where   dt                  = '2021-08-16'
	    and data.payment_status = '1602'
        and type                = 'update' 
        and array_contains(map_keys(old), 'payment_status')
) as payment 
    on detail.order_id = payment.order_id 
left join 
(
    select data.id, 
           data.province_id
    from ods_order_info_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as info 
    on detail.order_id = info.id 
left join 
(
    select data.order_detail_id, 
           data.activity_id, 
           data.activity_rule_id
    from ods_order_detail_activity_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as activity 
    on detail.id = activity.order_detail_id 
left join 
(
    select data.order_detail_id, 
           data.coupon_id
    from ods_order_detail_coupon_inc
    where (dt = '2021-08-16' or dt = date_add('2021-08-16', -1)) and (type = 'insert' or type = 'bootstrap-insert')
) as coupon 
    on detail.id = coupon.order_detail_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '11'
) as pay_dic 
    on payment.payment_type = pay_dic.dic_code 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '24'
) as src_dic on detail.source_type = src_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域退单事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_order_refund_inc;
create external table if not exists dwd_trade_order_refund_inc
(
    id                      string         comment '编号',
    user_id                 string         comment '用户 ID',
    order_id                string         comment '订单 ID',
    sku_id                  string         comment '商品 ID',
    province_id             string         comment '地区 ID',
    date_id                 string         comment '日期 ID',
    create_time             string         comment '退单时间',
    refund_type_code        string         comment '退单类型编码',
    refund_type_name        string         comment '退单类型名称',
    refund_reason_type_code string         comment '退单原因类型编码',
    refund_reason_type_name string         comment '退单原因类型名称',
    refund_reason_txt       string         comment '退单原因描述',
    refund_num              bigint         comment '退单件数',
    refund_amount           decimal(16, 2) comment '退单金额'
) comment '交易域退单事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_order_refund_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_trade_order_refund_inc partition (dt)
select refund.id,
       refund.user_id,
       refund.order_id,
       refund.sku_id,
       order_info.province_id,
       date_format(refund.create_time, 'yyyy-MM-dd') as date_id,
       refund.create_time,
       refund.refund_type                            as refund_type_code,
       type_dic.dic_name                             as refund_type_name,
       refund.refund_reason_type                     as refund_reason_type_code,
       reason_dic.dic_name                           as refund_reason_type_name,
       refund.refund_reason_txt,
       refund.refund_num,
       refund.refund_amount,
       date_format(refund.create_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id,
           data.user_id,
           data.order_id,
           data.sku_id,
           data.refund_type,
           data.refund_num,
           data.refund_amount,
           data.refund_reason_type,
           data.refund_reason_txt,
           data.create_time
    from ods_order_refund_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as refund left join 
(
    select data.id, 
           data.province_id
    from ods_order_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as order_info 
    on refund.order_id = order_info.id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '15'
) as type_dic 
    on refund.refund_type = type_dic.dic_code 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '13'
) as reason_dic on refund.refund_reason_type = reason_dic.dic_code;

-- 每日数据装载
insert overwrite table dwd_trade_order_refund_inc partition (dt = '2021-08-16')
select refund.id,
       refund.user_id,
       refund.order_id,
       refund.sku_id,
       order_info.province_id,
       date_format(refund.create_time, 'yyyy-MM-dd') as date_id,
       refund.create_time,
       refund.refund_type                            as refund_type_code,
       type_dic.dic_name                             as refund_type_name,
       refund.refund_reason_type                     as refund_reason_type_code,
       reason_dic.dic_name                           as refund_reason_type_name,
       refund.refund_reason_txt,
       refund.refund_num,
       refund.refund_amount
from 
(
    select data.id,
           data.user_id,
           data.order_id,
           data.sku_id,
           data.refund_type,
           data.refund_num,
           data.refund_amount,
           data.refund_reason_type,
           data.refund_reason_txt,
           data.create_time
    from ods_order_refund_info_inc
    where dt = '2021-08-16' and type = 'insert'
) as refund left join 
(
    select data.id, 
           data.province_id
    from ods_order_info_inc
    where                dt   = '2021-08-16' 
	                 and type = 'update' 
	    and data.order_status = '1005' 
		and array_contains(map_keys(old), 'order_status')
) as order_info 
    on refund.order_id = order_info.id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '15'
) as type_dic 
    on refund.refund_type = type_dic.dic_code 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '13'
) reason_dic on refund.refund_reason_type = reason_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域退款成功事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_refund_pay_suc_inc;
create external table if not exists dwd_trade_refund_pay_suc_inc
(
    id                string         comment '编号',
    user_id           string         comment '用户 ID',
    order_id          string         comment '订单编号',
    sku_id            string         comment 'SKU 编号',
    province_id       string         comment '地区 ID',
    payment_type_code string         comment '支付类型编码',
    payment_type_name string         comment '支付类型名称',
    date_id           string         comment '日期 ID',
    callback_time     string         comment '支付成功时间',
    refund_num        decimal(16, 2) comment '退款件数',
    refund_amount     decimal(16, 2) comment '退款金额'
) comment '交易域提交退款成功事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_refund_pay_suc_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt)
select refund_payment.id,
       order_info.user_id,
       refund_payment.order_id,
       refund_payment.sku_id,
       order_info.province_id,
       refund_payment.payment_type              as payment_type_code,
       base_dic.dic_name                        as payment_type_name,
       date_format(callback_time, 'yyyy-MM-dd') as date_id,
       refund_payment.callback_time,
       refund_info.refund_num,
       refund_payment.total_amount,
       date_format(callback_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id, 
           data.order_id, 
           data.sku_id, 
           data.payment_type, 
           data.callback_time, 
           data.total_amount
    from ods_refund_payment_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert' and data.refund_status = '1602'
) as refund_payment left join 
(
    select data.id, 
           data.user_id, 
           data.province_id
    from ods_order_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as order_info 
    on refund_payment.order_id = order_info.id 
left join 
(
    select data.order_id, 
           data.sku_id, 
           data.refund_num
    from ods_order_refund_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as refund_info 
    on      refund_payment.order_id = refund_info.order_id 
        and refund_payment.sku_id   = refund_info.sku_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '11'
) as base_dic on refund_payment.payment_type = base_dic.dic_code;

-- 每日数据装载
insert overwrite table dwd_trade_refund_pay_suc_inc partition (dt = '2021-08-16')
select refund_payment.id,
       order_info.user_id,
       refund_payment.order_id,
       refund_payment.sku_id,
       order_info.province_id,
       refund_payment.payment_type              as payment_type_code,
       base_dic.dic_name                        as payment_type_name,
       date_format(callback_time, 'yyyy-MM-dd') as date_id,
       refund_payment.callback_time,
       refund_info.refund_num,
       refund_payment.total_amount
from 
(
    select data.id, 
           data.order_id, 
           data.sku_id, 
           data.payment_type, 
           data.callback_time, 
           data.total_amount
    from ods_refund_payment_inc
    where   dt                 = '2021-08-16' 
	    and type               = 'update' 
		and data.refund_status = '1602'
		and array_contains(map_keys(old), 'refund_status')
) as refund_payment left join 
(
    select data.id, 
           data.user_id, 
           data.province_id
    from ods_order_info_inc
    where   dt                = '2021-08-16' 
	    and type              = 'update' 
		and data.order_status = '1006'
		and array_contains(map_keys(old), 'order_status')
) as order_info 
    on refund_payment.order_id = order_info.id 
left join 
(
    select data.order_id, 
           data.sku_id, 
           data.refund_num
    from ods_order_refund_info_inc
    where   dt                 = '2021-08-16' 
	    and type               = 'update' 
		and data.refund_status = '0705'
		and array_contains(map_keys(old), 'refund_status')
) as refund_info 
    on      refund_payment.order_id = refund_info.order_id 
        and refund_payment.sku_id   = refund_info.sku_id 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '11'
) as base_dic on refund_payment.payment_type = base_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 交易域购物车周期快照事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_trade_cart_full;
create external table if not exists dwd_trade_cart_full
(
    id       string comment '编号',
    user_id  string comment '用户 ID',
    sku_id   string comment '商品 ID',
    sku_name string comment '商品名称',
    sku_num  bigint comment '购物车件数'
) comment '交易域购物车周期快照事实表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' 
    stored as orc 
    location '/warehouse/dwd/dwd_trade_cart_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dwd_trade_cart_full partition (dt = '2021-08-15')
select id, 
       user_id, 
       sku_id, 
       sku_name, 
       sku_num
from ods_cart_info_full
where dt = '2021-08-15' and is_ordered = '0';


-- -------------------------------------------------------------------------------------------------
-- 工具域优惠券领取事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_tool_coupon_get_inc;
create external table if not exists dwd_tool_coupon_get_inc
(
    id        string comment '编号',
    coupon_id string comment '优惠券 ID',
    user_id   string comment 'USER_ ID',
    date_id   string comment '日期  ID',
    get_time  string comment '领取时间'
) comment '优惠券领取事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_tool_coupon_get_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       date_format(data.get_time, 'yyyy-MM-dd') as date_id,
       data.get_time,
       date_format(data.get_time, 'yyyy-MM-dd') as dt
from ods_coupon_use_inc
where dt = '2021-08-15' and type = 'bootstrap-insert';

-- 每日装载
insert overwrite table dwd_tool_coupon_get_inc partition (dt = '2021-08-16')
select data.id, 
       data.coupon_id, 
       data.user_id, 
       date_format(data.get_time, 'yyyy-MM-dd') as date_id, 
       data.get_time
from ods_coupon_use_inc
where dt = '2021-08-16' and type = 'insert';


-- -------------------------------------------------------------------------------------------------
-- 工具域优惠券使用（下单）：事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_tool_coupon_order_inc;
create external table if not exists dwd_tool_coupon_order_inc
(
    id         string comment '编号',
    coupon_id  string comment '优惠券 ID',
    user_id    string comment '用户 ID',
    order_id   string comment '订单 ID',
    date_id    string comment '日期 ID',
    order_time string comment '使用下单时间'
) comment '优惠券使用下单事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_tool_coupon_order_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') as date_id,
       data.using_time                            as order_time,
       date_format(data.using_time, 'yyyy-MM-dd') as dt
from ods_coupon_use_inc
where dt = '2021-08-15' and type = 'bootstrap-insert' and data.using_time is not null;

-- 每日装载
insert overwrite table dwd_tool_coupon_order_inc partition (dt = '2021-08-16')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.using_time, 'yyyy-MM-dd') as date_id,
       data.using_time                            as order_time
from ods_coupon_use_inc
where dt = '2021-08-16' and type = 'update' and array_contains(map_keys(old), 'using_time');


-- -------------------------------------------------------------------------------------------------
-- 工具域优惠券使用（支付）：事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_tool_coupon_pay_inc;
create external table if not exists dwd_tool_coupon_pay_inc
(
    id           string comment '编号',
    coupon_id    string comment '优惠券 ID',
    user_id      string comment '用户 ID',
    order_id     string comment '订单 ID',
    date_id      string comment '日期 ID',
    payment_time string comment '使用支付时间'
) comment '优惠券使用支付事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_tool_coupon_pay_inc/'
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_tool_coupon_pay_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') as date_id,
       data.used_time                            as payment_time,
       date_format(data.used_time, 'yyyy-MM-dd') as dt
from ods_coupon_use_inc
where dt = '2021-08-15' and type = 'bootstrap-insert' and data.used_time is not null;

-- 每日装载
insert overwrite table dwd_tool_coupon_pay_inc partition (dt = '2021-08-16')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') as date_id,
       data.used_time                            as payment_time
from ods_coupon_use_inc
where dt = '2021-08-16' and type = 'update' and array_contains(map_keys(old), 'used_time');


-- -------------------------------------------------------------------------------------------------
-- 互动域收藏商品事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_interaction_favor_add_inc;
create external table if not exists dwd_interaction_favor_add_inc
(
    id          string comment '编号',
    user_id     string comment '用户 ID',
    sku_id      string comment 'sku_ ID',
    date_id     string comment '日期 ID',
    create_time string comment '收藏时间'
) comment '收藏事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_interaction_favor_add_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') as date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd') as dt
from ods_favor_info_inc
where dt = '2021-08-15' and type = 'bootstrap-insert'; 

-- 每日装载
insert overwrite table dwd_interaction_favor_add_inc partition (dt = '2021-08-16')
select data.id, 
       data.user_id, 
       data.sku_id, 
       date_format(data.create_time, 'yyyy-MM-dd') as date_id, 
       data.create_time
from ods_favor_info_inc
where dt = '2021-08-16' and type = 'insert';


-- -------------------------------------------------------------------------------------------------
-- 互动域评价事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_interaction_comment_inc;
create external table if not exists dwd_interaction_comment_inc
(
    id            string comment '编号',
    user_id       string comment '用户 ID',
    sku_id        string comment 'SKU ID',
    order_id      string comment '订单 ID',
    date_id       string comment '日期 ID',
    create_time   string comment '评价时间',
    appraise_code string comment '评价编码',
    appraise_name string comment '评价名称'
) comment '评价事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_interaction_comment_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
insert overwrite table dwd_interaction_comment_inc partition (dt)
select comment_info.id,
       comment_info.user_id,
       comment_info.sku_id,
       comment_info.order_id,
       date_format(comment_info.create_time, 'yyyy-MM-dd') as date_id,
       comment_info.create_time,
       comment_info.appraise                               as appraise_code,
       base_dic.dic_name                                   as appraise_name,
       date_format(comment_info.create_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id, 
           data.user_id, 
           data.sku_id, 
           data.order_id, 
           data.create_time, 
           data.appraise
    from ods_comment_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert'
) as comment_info left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '12'
) as base_dic on comment_info.appraise = base_dic.dic_code;

-- 每日装载
insert overwrite table dwd_interaction_comment_inc partition (dt = '2021-08-16')
select comment_info.id,
       comment_info.user_id,
       comment_info.sku_id,
       comment_info.order_id,
       date_format(comment_info.create_time, 'yyyy-MM-dd') as date_id,
       comment_info.create_time,
       comment_info.appraise                               as appraise_code,
       base_dic.dic_name                                   as appraise_name
from 
(
    select data.id, 
           data.user_id, 
           data.sku_id, 
           data.order_id, 
           data.create_time, 
           data.appraise
    from ods_comment_info_inc
    where dt = '2021-08-16' and type = 'insert'
) as comment_info left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-16' and parent_code = '12'
) as base_dic on comment_info.appraise = base_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 流量域页面浏览事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_traffic_page_view_inc;
create external table if not exists dwd_traffic_page_view_inc
(
    province_id    string comment '省份 ID',
    brand          string comment '手机品牌',
    channel        string comment '渠道',
    is_new         string comment '是否首次启动',
    model          string comment '手机型号',
    mid_id         string comment '设备 ID',
    operate_system string comment '操作系统',
    user_id        string comment '会员 ID',
    version_code   string comment 'APP 版本号',
    page_item      string comment '目标 ID',
    page_item_type string comment '目标类型',
    last_page_id   string comment '上页类型',
    page_id        string comment '页面 ID',
    source_type    string comment '来源类型',
    date_id        string comment '日期 ID',
    view_time      string comment '跳入时间',
    session_id     string comment '所属会话 ID',
    during_time    bigint comment '持续时间毫秒'
) comment '页面日志表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_traffic_page_view_inc' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_page_view_inc partition (dt = '2021-08-15')
select base_province.province_id,
       log.brand,
       log.channel,
       log.is_new,
       log.model,
       log.mid_id,
       log.operate_system,
       log.user_id,
       log.version_code,
       log.page_item,
       log.page_item_type,
       log.last_page_id,
       log.page_id,
       log.source_type,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')                                                    as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                                           as view_time,
       concat(log.mid_id, '-', last_value(log.session_start_point, true) over (partition by log.mid_id order by log.ts)) as session_id,
       during_time
from 
(
    select common.ar                               as area_code,
           common.ba                               as brand,
           common.ch                               as channel,
           common.is_new                           as is_new,
           common.md                               as model,
           common.mid                              as mid_id,
           common.os                               as operate_system,
           common.uid                              as user_id,
           common.vc                               as version_code,
           page.during_time,
           page.item                               as page_item,
           page.item_type                          as page_item_type,
           page.last_page_id,
           page.page_id,
           page.source_type,
           ts,
           if(page.last_page_id is null, ts, null) as session_start_point
    from ods_log_inc
    where dt = '2021-08-15' and page is not null
) as log left join 
(
    select id        as province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) as base_province on log.area_code = base_province.area_code;


-- -------------------------------------------------------------------------------------------------
-- 流量域启动事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_traffic_start_inc;
create external table if not exists dwd_traffic_start_inc
(
    province_id     string comment '省份 ID',
    brand           string comment '手机品牌',
    channel         string comment '渠道',
    is_new          string comment '是否首次启动',
    model           string comment '手机型号',
    mid_id          string comment '设备 ID',
    operate_system  string comment '操作系统',
    user_id         string comment '会员 ID',
    version_code    string comment 'app版本号',
    entry           string comment 'icon手机图标 notice 通知',
    open_ad_id      string comment '广告页ID ',
    date_id         string comment '日期 ID',
    start_time      string comment '启动时间',
    loading_time_ms bigint comment '启动加载时间',
    open_ad_ms      bigint comment '广告总共播放时间',
    open_ad_skip_ms bigint comment '用户跳过广告时点'
) comment '启动日志表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_traffic_start_inc' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_start_inc partition (dt = '2021-08-15')
select base_province.province_id,
       log.brand,
       log.channel,
       log.is_new,
       log.model,
       log.mid_id,
       log.operate_system,
       log.user_id,
       log.version_code,
       log.entry,
       log.open_ad_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as start_time,
       log.loading_time                                                        as loading_time_ms,
       log.open_ad_ms,
       log.open_ad_skip_ms
from 
(
    select common.ar               as area_code,
           common.ba               as brand,
           common.ch               as channel,
           common.is_new,
           common.md               as model,
           common.mid              as mid_id,
           common.os               as operate_system,
           common.uid              as user_id,
           common.vc               as version_code,
           `start`.entry,
           `start`.loading_time,
           `start`.open_ad_id,
           `start`.open_ad_ms,
           `start`.open_ad_skip_ms,
           ts
    from ods_log_inc
    where dt = '2021-08-15' and `start` is not null
) as log left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) as base_province on log.area_code = base_province.area_code;


-- -------------------------------------------------------------------------------------------------
-- 流量域动作事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_traffic_action_inc;
create external table if not exists dwd_traffic_action_inc
(
    province_id      string comment '省份 ID',
    brand            string comment '手机品牌',
    channel          string comment '渠道',
    is_new           string comment '是否首次启动',
    model            string comment '手机型号',
    mid_id           string comment '设备 ID',
    operate_system   string comment '操作系统',
    user_id          string comment '会员 ID',
    version_code     string comment 'app版本号',
    during_time      bigint comment '持续时间毫秒',
    page_item        string comment '目标 ID',
    page_item_type   string comment '目标类型',
    last_page_id     string comment '上页类型',
    page_id          string comment '页面 ID',
    source_type      string comment '来源类型',
    action_id        string comment '动作 ID',
    action_item      string comment '目标 ID',
    action_item_type string comment '目标类型',
    date_id          string comment '日期 ID',
    action_time      string comment '动作发生时间'
) comment '动作日志表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_traffic_action_inc' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_action_inc partition (dt = '2021-08-15')
select base_province.province_id,
       log.brand,
       log.channel,
       log.is_new,
       log.model,
       log.mid_id,
       log.operate_system,
       log.user_id,
       log.version_code,
       log.during_time,
       log.page_item,
       log.page_item_type,
       log.last_page_id,
       log.page_id,
       log.source_type,
       log.action_id,
       log.action_item,
       log.action_item_type,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as action_time
from 
(
    select common.ar          as area_code,
           common.ba          as brand,
           common.ch          as channel,
           common.is_new,
           common.md          as model,
           common.mid         as mid_id,
           common.os          as operate_system,
           common.uid         as user_id,
           common.vc          as version_code,
           page.during_time,  
           page.item          as page_item,
           page.item_type     as page_item_type,
           page.last_page_id,
           page.page_id,
           page.source_type,
           action.action_id,
           action.item        as action_item,
           action.item_type   as action_item_type,
           action.ts
    from ods_log_inc lateral view explode(actions) tmp as action
    where dt = '2021-08-15' and actions is not null
) as log left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) base_province on log.area_code = base_province.area_code;


-- -------------------------------------------------------------------------------------------------
-- 流量域曝光事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_traffic_display_inc;
create external table if not exists dwd_traffic_display_inc
(
    province_id       string comment '省份 ID',
    brand             string comment '手机品牌',
    channel           string comment '渠道',
    is_new            string comment '是否首次启动',
    model             string comment '手机型号',
    mid_id            string comment '设备 ID',
    operate_system    string comment '操作系统',
    user_id           string comment '会员 ID',
    version_code      string comment 'app版本号',
    during_time       bigint comment 'app版本号',
    page_item         string comment '目标id ',
    page_item_type    string comment '目标类型',
    last_page_id      string comment '上页类型',
    page_id           string comment '页面ID ',
    source_type       string comment '来源类型',
    date_id           string comment '日期 ID',
    display_time      string comment '曝光时间',
    display_type      string comment '曝光类型',
    display_item      string comment '曝光对象id ',
    display_item_type string comment '曝光对象类型',
    display_order     bigint comment '曝光顺序',
    display_pos_id    bigint comment '曝光位置'
) comment '曝光日志表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_traffic_display_inc' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
insert overwrite table dwd_traffic_display_inc partition (dt = '2021-08-15')
select base_province.province_id,
       log.brand,
       log.channel,
       log.is_new,
       log.model,
       log.mid_id,
       log.operate_system,
       log.user_id,
       log.version_code,
       log.during_time,
       log.page_item,
       log.page_item_type,
       log.last_page_id,
       log.page_id,
       log.source_type,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as display_time,
       log.display_type,
       log.display_item,
       log.display_item_type,
       log.display_order,
       log.display_pos_id
from 
(
    select common.ar             as area_code,
           common.ba             as brand,
           common.ch             as channel,
           common.is_new,    
           common.md             as model,
           common.mid            as mid_id,
           common.os             as operate_system,
           common.uid            as user_id,
           common.vc             as version_code,
           page.during_time,    
           page.item             as page_item,
           page.item_type        as page_item_type,
           page.last_page_id,
           page.page_id,
           page.source_type,
           display.display_type,
           display.item          as display_item,
           display.item_type     as display_item_type,
           display.`order`        as display_order,
           display.pos_id        as display_pos_id,
           ts
    from ods_log_inc lateral view explode(displays) tmp as display
    where dt = '2021-08-15' and displays is not null
) as log left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) as base_province on log.area_code = base_province.area_code;


-- -------------------------------------------------------------------------------------------------
-- 流量域错误事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_traffic_error_inc;
create external table if not exists dwd_traffic_error_inc
(
    province_id     string                                                                           comment '地区编码',
    brand           string                                                                           comment '手机品牌',
    channel         string                                                                           comment '渠道',
    is_new          string                                                                           comment '是否首次启动',
    model           string                                                                           comment '手机型号',
    mid_id          string                                                                           comment '设备 ID',
    operate_system  string                                                                           comment '操作系统',
    user_id         string                                                                           comment '会员 ID',
    version_code    string                                                                           comment 'APP 版本号',
    page_item       string                                                                           comment '目标 ID',
    page_item_type  string                                                                           comment '目标类型',
    last_page_id    string                                                                           comment '上页类型',
    page_id         string                                                                           comment '页面 ID',
    source_type     string                                                                           comment '来源类型',
    entry           string                                                                           comment 'icon 手机图标  notice 通知',
    loading_time    string                                                                           comment '启动加载时间',
    open_ad_id      string                                                                           comment '广告页 ID',
    open_ad_ms      string                                                                           comment '广告总共播放时间',
    open_ad_skip_ms string                                                                           comment '用户跳过广告时点',
    actions         array<struct<action_id: string,    item: string, item_type: string, ts: bigint>> comment '动作信息',
    displays        array<struct<display_type: string, item: string, item_type: string,
                                 `order`: string,       pos_id: string>>                              comment '曝光信息',
    date_id         string                                                                           comment '日期  ID',
    error_time      string                                                                           comment '错误时间',
    error_code      string                                                                           comment '错误码',
    error_msg       string                                                                           comment '错误信息'
) comment '错误日志表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_traffic_error_inc' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
set hive.cbo.enable=false;
set hive.execution.engine=mr;
insert overwrite table dwd_traffic_error_inc partition (dt = '2021-08-15')
select base_province.province_id,
       log.brand,
       log.channel,
       log.is_new,
       log.model,
       log.mid_id,
       log.operate_system,
       log.user_id,
       log.version_code,
       log.page_item,
       log.page_item_type,
       log.last_page_id,
       log.page_id,
       log.source_type,
       log.entry,
       log.loading_time,
       log.open_ad_id,
       log.open_ad_ms,
       log.open_ad_skip_ms,
       log.actions,
       log.displays,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as error_time,
       log.error_code,
       log.error_msg
from 
(
    select common.ar             as area_code,
           common.ba             as brand,
           common.ch             as channel,
           common.is_new,
           common.md             as model,
           common.mid            as mid_id,
           common.os             as operate_system,
           common.uid            as user_id,
           common.vc             as version_code,
           page.during_time,     
           page.item             as page_item,
           page.item_type        as page_item_type,
           page.last_page_id,   
           page.page_id,
           page.source_type,
           `start`.entry,
           `start`.loading_time,
           `start`.open_ad_id,
           `start`.open_ad_ms,
           `start`.open_ad_skip_ms,
           actions,
           displays,
           err.error_code,
           err.msg               as error_msg,
           ts
    from ods_log_inc
    where dt = '2021-08-15' and err is not null
) as log left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) as base_province on log.area_code = base_province.area_code;
set hive.execution.engine=spark;


-- -------------------------------------------------------------------------------------------------
-- 用户域用户注册事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_user_register_inc;
create external table if not exists dwd_user_register_inc
(
    user_id        string comment '用户 ID',
    date_id        string comment '日期 ID',
    create_time    string comment '注册时间',
    channel        string comment '应用下载渠道',
    province_id    string comment '省份 ID',
    version_code   string comment '应用版本',
    mid_id         string comment '设备 ID',
    brand          string comment '设备品牌',
    model          string comment '设备型号',
    operate_system string comment '设备操作系统'
) comment '用户域用户注册事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_user_register_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 首日数据装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_user_register_inc partition (dt)
select user_info.user_id,
       date_format(user_info.create_time, 'yyyy-MM-dd') as date_id,
       user_info.create_time,
       log.channel,
       base_province.province_id,
       log.version_code,
       log.mid_id,
       log.brand,
       log.model,
       log.operate_system,
       date_format(user_info.create_time, 'yyyy-MM-dd') as dt
from 
(
    select data.id user_id, 
           data.create_time
    from ods_user_info_inc
    where dt = '2021-08-15' and type = 'bootstrap-insert' 
) user_info left join 
(
    select common.ar  as area_code,
           common.ba  as brand,
           common.ch  as channel,
           common.md  as model,
           common.mid as mid_id,
           common.os  as operate_system,
           common.uid as user_id,
           common.vc  as version_code
    from ods_log_inc
    where dt = '2021-08-15' and page.page_id = 'register' and common.uid is not null
) as log 
    on user_info.user_id = log.user_id 
left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) base_province on log.area_code = base_province.area_code;

-- 每日装载
insert overwrite table dwd_user_register_inc partition (dt = '2021-08-16')
select user_info.user_id,
       date_format(user_info.create_time, 'yyyy-MM-dd') as date_id,
       user_info.create_time,
       log.channel,
       base_province.province_id,
       log.version_code,
       log.mid_id,
       log.brand,
       log.model,
       log.operate_system
from 
(
    select data.id user_id, 
           data.create_time
    from ods_user_info_inc
    where dt = '2021-08-16' and type = 'insert' 
) user_info left join 
(
    select common.ar  as area_code,
           common.ba  as brand,
           common.ch  as channel,
           common.md  as model,
           common.mid as mid_id,
           common.os  as operate_system,
           common.uid as user_id,
           common.vc  as version_code
    from ods_log_inc
    where dt = '2021-08-16' and page.page_id = 'register' and common.uid is not null
) as log 
    on user_info.user_id = log.user_id 
left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-16'
) base_province on log.area_code = base_province.area_code;


-- -------------------------------------------------------------------------------------------------
-- 用户域用户登录事务事实表
-- -------------------------------------------------------------------------------------------------
drop table if exists dwd_user_login_inc;
create external table if not exists dwd_user_login_inc
(
    user_id        string comment '用户 ID',
    date_id        string comment '日期 ID',
    login_time     string comment '登录时间',
    channel        string comment '应用下载渠道',
    province_id    string comment '省份 ID',
    version_code   string comment '应用版本',
    mid_id         string comment '设备 ID',
    brand          string comment '设备品牌',
    model          string comment '设备型号',
    operate_system string comment '设备操作系统'
) comment '用户域用户登录事务事实表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dwd/dwd_user_login_inc/' 
    tblproperties ("orc.compress" = "snappy");

-- 数据装载
insert overwrite table dwd_user_login_inc partition (dt = '2021-08-15')
select user_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,
       date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,
       log.channel,
       base_province.province_id,
       log.version_code,
       log.mid_id,
       log.brand,
       log.model,
       log.operate_system
from 
(
    select v.user_id,
           v.channel,
           v.area_code,
           v.version_code,
           v.mid_id,
           v.brand,
           v.model,
           v.operate_system,
           v.ts
    from 
    (
        select u.user_id,
               u.channel,
               u.area_code,
               u.version_code,
               u.mid_id,
               u.brand,
               u.model,
               u.operate_system,
               u.ts,
               row_number() over (partition by u.session_id order by u.ts) as rn
        from 
        (
            select t.user_id,
                   t.channel,
                   t.area_code,
                   t.version_code,
                   t.mid_id,
                   t.brand,
                   t.model,
                   t.operate_system,
                   t.ts,
                   concat(t.mid_id, '-', last_value(t.session_start_point, true) over (partition by t.mid_id order by t.ts)) as session_id
            from 
            (
                select common.uid                              as user_id,
                       common.ch                               as channel,
                       common.ar                               as area_code,
                       common.vc                               as version_code,
                       common.mid                              as mid_id,
                       common.ba                               as brand,
                       common.md                               as model,
                       common.os                               as operate_system,
                       ts,
                       if(page.last_page_id is null, ts, null) as session_start_point
	            from ods_log_inc
	            where dt = '2021-08-15' and page is not null
            ) as t
        ) as u where user_id is not null
    ) as v where rn = 1
) as log left join 
(
    select id province_id, 
           area_code 
    from ods_base_province_full 
    where dt = '2021-08-15'
) as base_province on log.area_code = base_province.area_code;


-- DWD 层首日装载脚本：ods-dwd-init.sh all 2021-08-15
-- DWC 层每日装载脚本：ods-dwd.sh      all 2021-08-15

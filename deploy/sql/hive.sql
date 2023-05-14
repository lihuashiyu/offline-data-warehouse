-- -------------------------------------------------------------------------------------------------
-- 创建数据库
-- -------------------------------------------------------------------------------------------------
-- drop database if exists warehouse;
create database if not exists warehouse;
use warehouse;


-- -------------------------------------------------------------------------------------------------
-- ODS 层建表语句
-- -------------------------------------------------------------------------------------------------

-- 日志表
drop table if exists ods_log_inc;
create external table  if not exists ods_log_inc
(
	common   struct<ar: string, ba: string, ch: string, is_new: string, md: string, mid: string, os: string, uid: string, vc: string> comment '公共信息',
	page     struct<during_time: string, item: string, item_type: string, last_page_id: string, page_id: string, source_type: string> comment '页面信息',
	actions  array<struct<action_id: string,  item: string,  item_type: string,  ts: bigint>>                                         comment '动作信息',
	displays array<struct<display_type: string, item: string, item_type: string, `order`: string, pos_id: string>>                     comment '曝光信息',
	`start`   struct<entry: string, loading_time: bigint, open_ad_id: bigint, open_ad_ms: bigint, open_ad_skip_ms: bigint>             comment '启动信息',
	err      struct<error_code: bigint, msg: string>                                                                                  comment '错误信息',
	ts       bigint                                                                                                                   comment '时间戳'
) comment '活动信息表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_log_inc/';

-- 活动信息表（全量表）
drop table if exists ods_activity_info_full;
create external table  if not exists ods_activity_info_full
(
	id            string comment '活动id',
	activity_name string comment '活动名称',
	activity_type string comment '活动类型',
	activity_desc string comment '活动描述',
	start_time    string comment '开始时间',
	end_time      string comment '结束时间',
	create_time   string comment '创建时间'
) comment '活动信息表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_activity_info_full/';

-- 活动规则表（全量表）
drop table if exists ods_activity_rule_full;
create external table  if not exists ods_activity_rule_full
(
	id               string         comment '编号',
	activity_id      string         comment '类型',
	activity_type    string         comment '活动类型',
	condition_amount decimal(16, 2) comment '满减金额',
	condition_num    bigint         comment '满减件数',
	benefit_amount   decimal(16, 2) comment '优惠金额',
	benefit_discount decimal(16, 2) comment '优惠折扣',
	benefit_level    string         comment '优惠级别'
) comment '活动规则表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_activity_rule_full/';

-- 一级品类表（全量表）
drop table if exists ods_base_category1_full;
create external table  if not exists ods_base_category1_full
(
    id   string comment '编号',
    name string comment '分类名称'
) comment '一级品类表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category1_full/';

-- 二级品类表（全量表）
drop table if exists ods_base_category2_full;
create external table  if not exists ods_base_category2_full
(
	id           string comment '编号', 
	name         string comment '二级分类名称', 
	category1_id string comment '一级分类编号'
) comment '二级品类表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category2_full/';

-- 三级品类表（全量表）
drop table if exists ods_base_category3_full;
create external table  if not exists ods_base_category3_full
(
	id string           comment '编号', 
	name string         comment '三级分类名称', 
	category2_id string comment '二级分类编号'
) comment '三级品类表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category3_full/';

-- 编码字典表（全量表）
drop table if exists ods_base_dic_full;
create external table  if not exists ods_base_dic_full
(
    id           int    comment '编号',
	dic_code     string comment '编号',
	dic_name     string comment '编码名称',
	parent_code  string comment '父编号',
	create_time  string comment '创建日期',
	operate_time string comment '修改日期'
) comment '编码字典表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_dic_full/';

-- 省份表（全量表）
drop table if exists ods_base_province_full;
create external table  if not exists ods_base_province_full
(
	id         string comment '编号',
	name       string comment '省份名称',
	region_id  string comment '地区ID',
	area_code  string comment '地区编码',
	iso_code   string comment '旧版ISO-3166-2编码，供可视化使用',
	iso_3166_2 string comment '新版IOS-3166-2编码，供可视化使用'
) comment '省份表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_province_full/';

-- 地区表（全量表）
drop table if exists ods_base_region_full;
create external table  if not exists ods_base_region_full
(
    id          string comment '编号',
    region_name string comment '地区名称'
) comment '地区表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_region_full/';

-- 品牌表（全量表）
drop table if exists ods_base_trademark_full;
create external table  if not exists ods_base_trademark_full
(
	id       string comment '编号', 
	tm_name  string comment '品牌名称', 
	logo_url string comment '品牌 logo 的图片路径'
) comment '品牌表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_trademark_full/';

-- 购物车表（全量表）
drop table if exists ods_cart_info_full;
create external table  if not exists ods_cart_info_full
(
	id           string         comment '编号',
	user_id      string         comment '用户 ID',
	sku_id       string         comment 'SKU_ID',
	cart_price   decimal(16, 2) comment '放入购物车时价格',
	sku_num      bigint         comment '数量',
	img_url      bigint         comment '商品图片地址',
	sku_name     string         comment 'sku 名称 (冗余)',
	is_checked   string         comment '是否被选中',
	create_time  string         comment '创建时间',
	operate_time string         comment '修改时间',
	is_ordered   string         comment '是否已经下单',
	order_time   string         comment '下单时间',
	source_type  string         comment '来源类型',
	source_id    string         comment '来源编号'
) comment '购物车全量表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_cart_info_full/';

-- 优惠券信息表（全量表）
drop table if exists ods_coupon_info_full;
create external table  if not exists ods_coupon_info_full
(
	id               string         comment '购物券编号',
	coupon_name      string         comment '购物券名称',
	coupon_type      string         comment '购物券类型：1，现金券；2，折扣券；3，满减券；4，满件打折券',
	condition_amount decimal(16, 2) comment '满额数',
	condition_num    bigint         comment '满件数',
	activity_id      string         comment '活动编号',
	benefit_amount   decimal(16, 2) comment '减金额',
	benefit_discount decimal(16, 2) comment '折扣',
	create_time      string         comment '创建时间',
	range_type       string         comment '范围类型：1，商品；2，品类；3，品牌',
	limit_num        bigint         comment '最多领用次数',
	taken_count      bigint         comment '已领用次数',
	start_time       string         comment '开始领取时间',
	end_time         string         comment '结束领取时间',
	operate_time     string         comment '修改时间',
	expire_time      string         comment '过期时间'
) comment '优惠券信息表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_coupon_info_full/';

-- 商品平台属性表（全量表）
drop table if exists ods_sku_attr_value_full;
create external table  if not exists ods_sku_attr_value_full
(
	id         string comment '编号',
	attr_id    string comment '平台属性 ID',
	value_id   string comment '平台属性值 ID',
	sku_id     string comment '商品 ID',
	attr_name  string comment '平台属性名称',
	value_name string comment '平台属性值名称'
) comment 'SKU 平台属性表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_attr_value_full/';

-- 商品表（全量表）
drop table if exists ods_sku_info_full;
create external table  if not exists ods_sku_info_full
(
	id              string         comment 'SKU_ID',
	spu_id          string         comment 'SPU_ID',
	price           decimal(16, 2) comment '价格',
	sku_name        string         comment '商品名称',
	sku_desc        string         comment '商品描述',
	weight          decimal(16, 2) comment '重量',
	tm_id           string         comment '品牌 ID',
	category3_id    string         comment '品类 ID',
	sku_default_igm string         comment '商品图片地址',
	is_sale         string         comment '是否在售',
	create_time     string         comment '创建时间'
) comment 'SKU 商品表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_info_full/';

-- 商品销售属性值表（全量表）
drop table if exists ods_sku_sale_attr_value_full;
create external table  if not exists ods_sku_sale_attr_value_full
(
	id                   string comment '编号',
	sku_id               string comment 'SKU_ID',
	spu_id               string comment 'SPU_ID',
	sale_attr_value_id   string comment '销售属性值id',
	sale_attr_id         string comment '销售属性id',
	sale_attr_name       string comment '销售属性名称',
	sale_attr_value_name string comment '销售属性值名称'
) comment 'sku销售属性名称' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_sale_attr_value_full/';

--  SPU 表（全量表）
drop table if exists ods_spu_info_full;
create external table  if not exists ods_spu_info_full
(
	id           string comment 'SPU_ID',
	spu_name     string comment 'SPU 名称',
	description  string comment '描述信息',
	category3_id string comment '品类 ID',
	tm_id        string comment '品牌 ID'
) comment 'SPU商品表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_spu_info_full/';

-- 购物车表（增量表）
drop table if exists ods_cart_info_inc;
create external table  if not exists ods_cart_info_inc
(
	type string                                                                                            comment '变动类型',
	ts   bigint                                                                                            comment '变动时间',
	data struct<id: string,          user_id: string,      sku_id: string,     cart_price: decimal(16, 2),
	           sku_num: bigint,      img_url: string,      sku_name: string,   is_checked: string,  
	           create_time: string,  operate_time: string, is_ordered: string, order_time: string,  
	           source_type: string,  source_id: string>                                                    comment '数据',
	old  map<string, string>                                                                               comment '旧值'
) comment '购物车增量表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_cart_info_inc/';

-- 评论表（增量表）
drop table if exists ods_comment_info_inc;
create external table  if not exists ods_comment_info_inc
(
	type string                                                                                   comment '变动类型',
	ts   bigint                                                                                   comment '变动时间',
	data struct<id: string,          user_id: string,     nick_name: string,    head_img: string,  
	            sku_id: string,      spu_id: string,      order_id: string,     appraise: string,  
	            comment_txt: string, create_time: string, operate_time :string>                   comment '数据',
	old  map<string, string>                                                                      comment '旧值'
) comment '评价表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_comment_info_inc/';

-- 优惠券领用表（增量表）
drop table if exists ods_coupon_use_inc;
create external table  if not exists ods_coupon_use_inc
(
	type string                                                                  comment '变动类型',
	ts   bigint                                                                  comment '变动时间',
	data struct<id: string,         coupon_id: string,     user_id: string,  
	            order_id: string,   coupon_status: string, get_time: string,  
	            using_time: string, used_time: string,     expire_time :string>  comment '数据',
	old  map<string, string>                                                     comment '旧值'
) comment '优惠券领用表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_coupon_use_inc/';

-- 收藏表（增量表）
drop table if exists ods_favor_info_inc;
create external table  if not exists ods_favor_info_inc
(
	type string                                                                              comment '变动类型',
	ts   bigint                                                                              comment '变动时间',
	data struct<id: string,        user_id: string,     sku_id: string,      spu_id: string,   
	            is_cancel: string, create_time: string, cancel_time: string>                 comment '数据',
	old  map<string, string>                                                                 comment '旧值'
) comment '收藏表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_favor_info_inc/';

-- 订单明细表（增量表）
drop table if exists ods_order_detail_inc;
create external table  if not exists ods_order_detail_inc
(
	type string                                                                                                                  comment '变动类型',
	ts   bigint                                                                                                                  comment '变动时间',
	data struct<id: string,                          order_id: string,                   sku_id: string,
	            sku_name: string,                    img_url: string,                    order_price: decimal(16, 2),
	            sku_num: bigint,                     create_time: string,                source_type: string, 
	            source_id: string,                   split_total_amount: decimal(16, 2), split_activity_amount: decimal(16, 2), 
	            split_coupon_amount: decimal(16, 2)>                                                                            comment '数据',
	old  map<string, string>                                                                                                    comment '旧值'
) comment '订单明细表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_detail_inc/';

-- 订单明细活动关联表（增量表）
drop table if exists ods_order_detail_activity_inc;
create external table  if not exists ods_order_detail_activity_inc
(
	type string                                                                                             comment '变动类型',
	ts   bigint                                                                                             comment '变动时间',
	data struct<id: string,               order_id: string,  order_detail_id: string,  activity_id: string,  
	            activity_rule_id: string, sku_id: string,    create_time : string>                          comment '数据',
	old  map<string, string>                                                                                comment '旧值'
) comment '订单明细活动关联表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_detail_activity_inc/';

-- 订单明细优惠券关联表（增量表）
drop table if exists ods_order_detail_coupon_inc;
create external table  if not exists ods_order_detail_coupon_inc
(
	type string                                                                                       comment '变动类型',
	ts   bigint                                                                                       comment '变动时间',
	data struct<id: string,            order_id: string, order_detail_id: string,  coupon_id: string,  
	            coupon_use_id: string, sku_id: string,   create_time: string>                         comment '数据',
	old  map<string, string>                                                                          comment '旧值'
) comment '订单明细优惠券关联表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_detail_coupon_inc/';

-- 订单表（增量表）
drop table if exists ods_order_info_inc;
create external table  if not exists ods_order_info_inc
(
	type string                                                                                                                      comment '变动类型',
	ts   bigint                                                                                                                      comment '变动时间',
	data struct<id: string,                            consignee: string,                      consignee_tel: string,  
	            total_amount: decimal(16, 2),          order_status: string,                   user_id: string,  
	            payment_way: string,                   delivery_address: string,               order_comment: string,  
	            out_trade_no: string,                  trade_body: string,                     create_time: string,  
	            operate_time: string,                  expire_time: string,                    process_status: string,  
	            tracking_no: string,                   parent_order_id: string ,               img_url: string,  
	            province_id: string,                   activity_reduce_amount: decimal(16, 2), coupon_reduce_amount: decimal(16, 2),
	            original_total_amount: decimal(16, 2), freight_fee: decimal(16, 2),            freight_fee_reduce: decimal(16, 2),
	            refundable_time: decimal(16, 2)>                                                                                     comment '数据',
	old  map<string, string>                                                                                                         comment '旧值'
) comment '订单表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_info_inc/';

-- 退单表（增量表）
drop table if exists ods_order_refund_info_inc;
create external table  if not exists ods_order_refund_info_inc
(
	type string                                                                                       comment '变动类型',
	ts   bigint                                                                                       comment '变动时间',
	data struct<id: string,                    user_id: string,            order_id: string,  
	            sku_id: string,                refund_type: string,        refund_num: bigint,
	            refund_amount: decimal(16, 2), refund_reason_type: string, refund_reason_txt: string,  
	            refund_status: string,         create_time: string>                                   comment '数据',
	old  map<string, string>                                                                          comment '旧值'
) comment '退单表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_refund_info_inc/';

-- 订单状态流水表（增量表）
drop table if exists ods_order_status_log_inc;
create external table  if not exists ods_order_status_log_inc
(
	type string                                                                           comment '变动类型',
	ts   bigint                                                                           comment '变动时间',
	data struct<id: string, order_id: string, order_status: string, operate_time: string> comment '数据',
	old  map<string, string>                                                              comment '旧值'
) comment '退单表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_status_log_inc/';

-- 支付表（增量表）
drop table if exists ods_payment_info_inc;
create external table  if not exists ods_payment_info_inc
(
	type string                                                                                comment '变动类型',
	ts   bigint                                                                                comment '变动时间',
	data struct<id: string,                   out_trade_no: string,  order_id: string,  
	            user_id: string,              payment_type: string,  trade_no: string,  
	            total_amount: decimal(16, 2), subject: string,       payment_status: string,  
	            create_time: string,          callback_time: string, callback_content: string> comment '数据',
	old  map<string, string>                                                                   comment '旧值'
) comment '支付表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_payment_info_inc/';

-- 退款表（增量表）
drop table if exists ods_refund_payment_inc;
create external table  if not exists ods_refund_payment_inc
(
	type string                                                                                comment '变动类型',
	ts   bigint                                                                                comment '变动时间',
	data struct<id: string,                   out_trade_no: string,  order_id: string,  
	            sku_id: string,               payment_type: string,  trade_no: string,  
	            total_amount: decimal(16, 2), subject: string,       refund_status: string, 
	            create_time: string,          callback_time: string, callback_content: string> comment '数据',
	old  map<string, string>                                                                   comment '旧值'
) comment '退款表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_refund_payment_inc/';

-- 用户表（增量表）
drop table if exists ods_user_info_inc;
create external table  if not exists ods_user_info_inc
(
	type string                                                                 comment '变动类型',
	ts   bigint                                                                 comment '变动时间',
	data struct<id: string,           login_name: string,  nick_name: string,  
	            passwd: string,       name: string,        phone_num: string,  
	            email: string,        head_img: string,    user_level: string,  
	            birthday: string,     gender: string,      create_time: string,  
	            operate_time: string, `status`: string>                          comment '数据',
	old  map<string, string>                                                    comment '旧值'
) comment '用户表' 
    partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_user_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- DIM 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 商品维度表
drop table if exists dim_sku_full;
create external table  if not exists dim_sku_full
(
    id                   string                                                              comment 'SKU_ID',
    price                decimal(16, 2)                                                      comment '商品价格',
    sku_name             string                                                              comment '商品名称',
    sku_desc             string                                                              comment '商品描述',
    weight               decimal(16, 2)                                                      comment '重量',
    is_sale              boolean                                                             comment '是否在售',
    spu_id               string                                                              comment 'SPU 编号',
    spu_name             string                                                              comment 'SPU 名称',
    category3_id         string                                                              comment '三级分类 ID',
    category3_name       string                                                              comment '三级分类名称',
    category2_id         string                                                              comment '二级分类 ID',
    category2_name       string                                                              comment '二级分类名称',
    category1_id         string                                                              comment '一级分类 ID',
    category1_name       string                                                              comment '一级分类名称',
    tm_id                string                                                              comment '品牌 ID',
    tm_name              string                                                              comment '品牌名称',
    sku_attr_values      array<struct<attr_id: string,        value_id: string,           
                                      attr_name: string,      value_name: string>>           comment '平台属性',
    sku_sale_attr_values array<struct<sale_attr_id: string,   sale_attr_value_id: string, 
                                      sale_attr_name: string, sale_attr_value_name: string>> comment '销售属性',
    create_time          string                                                              comment '创建时间'
) comment '商品维度表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dim/dim_sku_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 优惠券维度表
drop table if exists dim_coupon_full;
create external table  if not exists dim_coupon_full
(
    id               string         comment '购物券编号',
    coupon_name      string         comment '购物券名称',
    coupon_type_code string         comment '购物券类型编码',
    coupon_type_name string         comment '购物券类型名称',
    condition_amount decimal(16, 2) comment '满额数',
    condition_num    bigint         comment '满件数',
    activity_id      string         comment '活动编号',
    benefit_amount   decimal(16, 2) comment '减金额',
    benefit_discount decimal(16, 2) comment '折扣',
    benefit_rule     string         comment '优惠规则:满元 * 减 * 元，满 * 件打 * 折',
    create_time      string         comment '创建时间',
    range_type_code  string         comment '优惠范围类型编码',
    range_type_name  string         comment '优惠范围类型名称',
    limit_num        bigint         comment '最多领取次数',
    taken_count      bigint         comment '已领取次数',
    start_time       string         comment '可以领取的开始日期',
    end_time         string         comment '可以领取的结束日期',
    operate_time     string         comment '修改时间',
    expire_time      string         comment '过期时间'
) comment '优惠券维度表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dim/dim_coupon_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 活动维度表
drop table if exists dim_activity_full;
create external table  if not exists dim_activity_full
(
    activity_rule_id   string         comment '活动规则 ID',
    activity_id        string         comment '活动 ID',
    activity_name      string         comment '活动名称',
    activity_type_code string         comment '活动类型编码',
    activity_type_name string         comment '活动类型名称',
    activity_desc      string         comment '活动描述',
    start_time         string         comment '开始时间',
    end_time           string         comment '结束时间',
    create_time        string         comment '创建时间',
    condition_amount   decimal(16, 2) comment '满减金额',
    condition_num      bigint         comment '满减件数',
    benefit_amount     decimal(16, 2) comment '优惠金额',
    benefit_discount   decimal(16, 2) comment '优惠折扣',
    benefit_rule       string         comment '优惠规则',
    benefit_level      string         comment '优惠级别'
) comment '活动信息表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dim/dim_activity_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 地区维度表
drop table if exists dim_province_full;
create external table  if not exists dim_province_full
(
    id            string comment 'ID',
    province_name string comment '省市名称',
    area_code     string comment '地区编码',
    iso_code      string comment '旧版 ISO-3166-2 编码，供可视化使用',
    iso_3166_2    string comment '新版 IOS-3166-2 编码，供可视化使用',
    region_id     string comment '地区 ID',
    region_name   string comment '地区名称'
) comment '地区维度表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dim/dim_province_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 日期维度表
drop table if exists dim_date;
create external table  if not exists dim_date
(
    date_id    string comment '日期 ID',
    week_id    string comment '周 ID,一年中的第几周',
    week_day   string comment '周几',
    day        string comment '每月的第几天',
    month      string comment '一年中的第几月',
    quarter    string comment '一年中的第几季度',
    year       string comment '年份',
    is_workday string comment '是否是工作日',
    holiday_id string comment '节假日'
) comment '时间维度表' 
    stored as orc 
    location '/warehouse/dim/dim_date/' 
    tblproperties ('orc.compress' = 'snappy');

-- 用户维度表
drop table if exists dim_user_zip;
create external table  if not exists dim_user_zip
(
    id           string comment '用户 ID',
    login_name   string comment '用户名称',
    nick_name    string comment '用户昵称',
    name         string comment '用户姓名',
    phone_num    string comment '手机号码',
    email        string comment '邮箱',
    user_level   string comment '用户等级',
    birthday     string comment '生日',
    gender       string comment '性别',
    create_time  string comment '创建时间',
    operate_time string comment '操作时间',
    start_date   string comment '开始日期',
    end_date     string comment '结束日期'
) comment '用户表' 
    partitioned by (dt string) 
    stored as orc 
    location '/warehouse/dim/dim_user_zip/' 
    tblproperties ('orc.compress' = 'snappy');


-- -------------------------------------------------------------------------------------------------
-- DWD 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 交易域加购事务事实表
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

-- 交易域下单事务事实表
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

-- 交易域取消订单事务事实表
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

-- 交易域支付成功事务事实表
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

-- 交易域退单事务事实表
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

-- 交易域退款成功事务事实表
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

-- 交易域购物车周期快照事实表
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

-- 工具域优惠券领取事务事实表
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

-- 工具域优惠券使用（下单）：事务事实表
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

-- 工具域优惠券使用（支付）：事务事实表
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

-- 互动域收藏商品事务事实表
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

-- 互动域评价事务事实表
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

-- 流量域页面浏览事务事实表
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

-- 流量域启动事务事实表
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

-- 流量域动作事务事实表
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

-- 流量域曝光事务事实表
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

-- 流量域错误事务事实表
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

-- 用户域用户注册事务事实表
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

-- 用户域用户登录事务事实表
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


-- -------------------------------------------------------------------------------------------------
-- DWS 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度订单最近 1 日汇总表
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

-- 交易域用户商品粒度退单最近 1 日汇总表
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

-- 交易域用户粒度订单最近 1 日汇总表
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

-- 交易域用户粒度加购最近 1 日汇总表
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

-- 交易域用户粒度支付最近 1 日汇总表
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

-- 最近 1 交易域用户粒度退单最近 1 日汇总表
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

-- 交易域省份粒度订单最近 1 日汇总表
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

-- 流量域会话粒度页面浏览最近 1 日汇总表
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

-- 流量域访客页面粒度页面浏览最近 1 日汇总表
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

-- 最近 N 日汇总表：交易域用户商品粒度订单最近 N 日汇总表
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

-- 交易域用户商品粒度退单最近 N 日汇总表
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

-- 交易域用户粒度订单最近 N 日汇总表
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

-- 交易域用户粒度加购最近 N 日汇总表
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

-- 交易域用户粒度支付最近 N 日汇总表
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

-- 交易域用户粒度退单最近 N 日汇总表
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

-- 交易域省份粒度订单最近 N 日汇总表
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

-- 交易域优惠券粒度订单最近 N 日汇总表
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

-- 交易域活动粒度订单最近 N 日汇总表
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

-- 流量域访客页面粒度页面浏览最近 N 日汇总表
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

-- 历史至今汇总表：交易域用户粒度订单历史至今汇总表
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

-- 交易域用户粒度支付历史至今汇总表
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

-- 用户域用户粒度登录历史至今汇总表
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


-- -------------------------------------------------------------------------------------------------
-- ADS 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 各渠道流量统计
drop table if exists ads_traffic_stats_by_channel;
create external table if not exists ads_traffic_stats_by_channel
(
    dt               string         comment '统计日期',
    recent_days      bigint         comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    channel          string         comment '渠道',
    uv_count         bigint         comment '访客人数',
    avg_duration_sec bigint         comment '会话平均停留时长，单位为秒',
    avg_page_count   bigint         comment '会话平均浏览页面数',
    sv_count         bigint         comment '会话数',
    bounce_rate      decimal(16, 2) comment '跳出率'
) comment '各渠道流量统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_traffic_stats_by_channel/';

-- 路径分析(页面单跳)
drop table if exists ads_page_path;
create external table if not exists ads_page_path
(
    dt          string comment '统计日期',
    recent_days bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    source      string comment '跳转起始页面ID',
    target      string comment '跳转终到页面ID',
    path_count  bigint comment '跳转次数'
) comment '页面浏览路径分析' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_page_path/';

-- 用户变动统计
drop table if exists ads_user_change;
create external table if not exists ads_user_change
(
    dt string               comment '统计日期', 
    user_churn_count bigint comment '流失用户数(新增)', 
    user_back_count  bigint comment '回流用户数'
) comment '用户变动统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_user_change/';

-- 用户留存率
drop table if exists ads_user_retention;
create external table if not exists ads_user_retention
(
    dt              string         comment '统计日期',
    create_date     string         comment '用户新增日期',
    retention_day   int            comment '截至当前日期留存天数',
    retention_count bigint         comment '留存用户数量',
    new_user_count  bigint         comment '新增用户数量',
    retention_rate  decimal(16, 2) comment '留存率'
) comment '用户留存率' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_user_retention/';

-- 用户新增活跃统计
drop table if exists ads_user_stats;
create external table if not exists ads_user_stats
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近 N 日：1、最近 1 日，7、最近 7 日，30、最近 30 日',
    new_user_count    bigint comment '新增用户数',
    active_user_count bigint comment '活跃用户数'
) comment '用户新增活跃统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_user_stats/';

-- 用户行为漏斗分析
drop table if exists ads_user_action;
create external table if not exists ads_user_action
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    home_count        bigint comment '浏览首页人数',
    good_detail_count bigint comment '浏览商品详情页人数',
    cart_count        bigint comment '加入购物车人数',
    order_count       bigint comment '下单人数',
    payment_count     bigint comment '支付人数'
) comment '漏斗分析' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_user_action/';

-- 新增交易用户统计
drop table if exists ads_new_buyer_stats;
create external table if not exists ads_new_buyer_stats
(
    dt                     string comment '统计日期',
    recent_days            bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    new_order_user_count   bigint comment '新增下单人数',
    new_payment_user_count bigint comment '新增支付人数'
) comment '新增交易用户统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_new_buyer_stats/';

-- 最近 7/30 日各品牌复购率
drop table if exists ads_repeat_purchase_by_tm;
create external table if not exists ads_repeat_purchase_by_tm
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近天数：7、最近 7 天，30、最近 30 天',
    tm_id             string comment '品牌 ID',
    tm_name           string comment '品牌名称',
    order_repeat_rate decimal(16, 2) comment '复购率'
) comment '各品牌复购率统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_repeat_purchase_by_tm/';

-- 各品牌商品交易统计
drop table if exists ads_trade_stats_by_tm;
create external table if not exists ads_trade_stats_by_tm
(
    dt                      string comment '统计日期',
    recent_days             bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    tm_id                   string comment '品牌ID',
    tm_name                 string comment '品牌名称',
    order_count             bigint comment '订单数',
    order_user_count        bigint comment '订单人数',
    order_refund_count      bigint comment '退单数',
    order_refund_user_count bigint comment '退单人数'
) comment '各品牌商品交易统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_trade_stats_by_tm/';

-- 各品类商品交易统计
drop table if exists ads_trade_stats_by_cate;
create external table if not exists ads_trade_stats_by_cate
(
    dt                      string comment '统计日期',
    recent_days             bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    category1_id            string comment '一级分类id',
    category1_name          string comment '一级分类名称',
    category2_id            string comment '二级分类id',
    category2_name          string comment '二级分类名称',
    category3_id            string comment '三级分类id',
    category3_name          string comment '三级分类名称',
    order_count             bigint comment '订单数',
    order_user_count        bigint comment '订单人数',
    order_refund_count      bigint comment '退单数',
    order_refund_user_count bigint comment '退单人数'
) comment '各分类商品交易统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_trade_stats_by_cate/';

-- 各分类商品购物车存量 Top3
drop table if exists ads_sku_cart_num_top3_by_cate;
create external table if not exists ads_sku_cart_num_top3_by_cate
(
    dt             string comment '统计日期',
    category1_id   string comment '一级分类 ID',
    category1_name string comment '一级分类名称',
    category2_id   string comment '二级分类 ID',
    category2_name string comment '二级分类名称',
    category3_id   string comment '三级分类 ID',
    category3_name string comment '三级分类名称',
    sku_id         string comment '商品 ID',
    sku_name       string comment '商品名称',
    cart_num       bigint comment '购物车中商品数量',
    rk             bigint comment '排名'
) comment '各分类商品购物车存量 Top10' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_sku_cart_num_top3_by_cate/';

-- 交易主题：交易综合统计
drop table if exists ads_trade_stats;
create external table if not exists ads_trade_stats
(
    dt                      string         comment '统计日期',
    recent_days             bigint         comment '最近天数,1:最近1日,7:最近7天,30:最近30天',
    order_total_amount      decimal(16, 2) comment '订单总额,GMV',
    order_count             bigint         comment '订单数',
    order_user_count        bigint         comment '下单人数',
    order_refund_count      bigint         comment '退单数',
    order_refund_user_count bigint         comment '退单人数'
) comment '交易统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_trade_stats/';

-- 各省份交易统计
drop table if exists ads_order_by_province;
create external table if not exists ads_order_by_province
(
    dt                 string         comment '统计日期',
    recent_days        bigint         comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    province_id        string         comment '省份ID',
    province_name      string         comment '省份名称',
    area_code          string         comment '地区编码',
    iso_code           string         comment '国际标准地区编码',
    iso_code_3166_2    string         comment '国际标准地区编码',
    order_count        bigint         comment '订单数',
    order_total_amount decimal(16, 2) comment '订单金额'
) comment '各地区订单统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_order_by_province/';

-- 优惠券主题：最近 30天发布的优惠券的补贴率
drop table if exists ads_coupon_stats;
create external table if not exists ads_coupon_stats
(
    dt          string         comment '统计日期',
    coupon_id   string         comment '优惠券ID',
    coupon_name string         comment '优惠券名称',
    start_date  string         comment '发布日期',
    rule_name   string         comment '优惠规则，例如满 100 元减 10 元',
    reduce_rate decimal(16, 2) comment '补贴率'
) comment '优惠券统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_coupon_stats/';

-- 活动主题：最近 30 天发布的活动的补贴率
drop table if exists ads_activity_stats;
create external table if not exists ads_activity_stats
(
    dt            string         comment '统计日期',
    activity_id   string         comment '活动 ID',
    activity_name string         comment '活动名称',
    start_date    string         comment '活动开始日期',
    reduce_rate   decimal(16, 2) comment '补贴率'
) comment '活动统计' 
    row format delimited fields terminated by '\t' 
    location '/warehouse/ads/ads_activity_stats/';


-- -------------------------------------------------------------------------------------------------
-- TMP 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 日期维度表
drop table if exists tmp_dim_date_info;
create external table  if not exists tmp_dim_date_info
(
    date_id    string comment '日',
    week_id    string comment '周 ID',
    week_day   string comment '周几',
    day        string comment '每月的第几天',
    month      string comment '第几月',
    quarter    string comment '第几季度',
    year       string comment '年',
    is_workday string comment '是否是工作日',
    holiday_id string comment '节假日'
) comment '时间维度表' row format delimited fields terminated by '\t' 
    location '/warehouse/tmp/tmp_dim_date_info/';

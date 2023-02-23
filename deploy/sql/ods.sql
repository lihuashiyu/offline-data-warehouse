drop database if exists warehouse;
create database if not exists warehouse;
use warehouse;
show tables ;

-- -------------------------------------------------------------------------------------------------
-- 日志表
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_log_inc;
create external table  if not exists ods_log_inc
(
	common   struct<ar: string, ba: string,  ch: string, is_new: string, md: string, mid: string, os: string, uid: string, vc:string> comment '公共信息',
	page     struct<during_time: string, item: string, item_type: string, last_page_id: string, page_id: string, source_type:string>  comment '页面信息',
	actions  array<struct<action_id: string,  item: string,  item_type: string,  ts:bigint>>                                          comment '动作信息',
	displays array<struct<display_type: string, item: string, item_type: string, `order`: string, pos_id:string>>                      comment '曝光信息',
	`start`   struct<entry: string, loading_time :bigint,open_ad_id :bigint,open_ad_ms :bigint,open_ad_skip_ms:bigint>                 comment '启动信息',
	err      struct<error_code: bigint, msg: string>                                                                                  comment '错误信息',
	ts       bigint                                                                                                                   comment '时间戳'
) comment '活动信息表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_log_inc/';

-- 数据装载
load data inpath '/warehouse/origin/mock/2021-08-15' into table ods_log_inc partition(dt='2021-08-15');
-- 每日数据装载脚本：hdfs_to_ods_log.sh 2021-08-15


-- -------------------------------------------------------------------------------------------------
-- 活动信息表（全量表）
-- -------------------------------------------------------------------------------------------------
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
) comment '活动信息表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_activity_info_full/';


-- -------------------------------------------------------------------------------------------------
-- 活动规则表（全量表）
-- -------------------------------------------------------------------------------------------------
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
) comment '活动规则表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_activity_rule_full/';


-- -------------------------------------------------------------------------------------------------
-- 一级品类表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_category1_full;
create external table  if not exists ods_base_category1_full
(
    id   string comment '编号',
    name string comment '分类名称'
) comment '一级品类表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category1_full/';


-- -------------------------------------------------------------------------------------------------
-- 二级品类表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_category2_full;
create external table  if not exists ods_base_category2_full
(
	id string           comment '编号', 
	name string         comment '二级分类名称', 
	category1_id string comment '一级分类编号'
) comment '二级品类表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category2_full/';


-- -------------------------------------------------------------------------------------------------
-- 三级品类表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_category3_full;
create external table  if not exists ods_base_category3_full
(
	id string           comment '编号', 
	name string         comment '三级分类名称', 
	category2_id string comment '二级分类编号'
) comment '三级品类表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_category3_full/';


-- -------------------------------------------------------------------------------------------------
-- 编码字典表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_dic_full;
create external table  if not exists ods_base_dic_full
(
    id           int    comment '编号',
	dic_code     string comment '编号',
	dic_name     string comment '编码名称',
	parent_code  string comment '父编号',
	create_time  string comment '创建日期',
	operate_time string comment '修改日期'
) comment '编码字典表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_dic_full/';

load data inpath '/warehouse/origin/db/base_dic_full/' into table ods_base_dic_full partition(dt='2021-08-15');

-- -------------------------------------------------------------------------------------------------
-- 省份表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_province_full;
create external table  if not exists ods_base_province_full
(
	id         string comment '编号',
	name       string comment '省份名称',
	region_id  string comment '地区ID',
	area_code  string comment '地区编码',
	iso_code   string comment '旧版ISO-3166-2编码，供可视化使用',
	iso_3166_2 string comment '新版IOS-3166-2编码，供可视化使用'
) comment '省份表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_province_full/';


-- -------------------------------------------------------------------------------------------------
-- 地区表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_region_full;
create external table  if not exists ods_base_region_full
(
    id          string comment '编号',
    region_name string comment '地区名称'
) comment '地区表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_region_full/';


-- -------------------------------------------------------------------------------------------------
-- 品牌表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_base_trademark_full;
create external table  if not exists ods_base_trademark_full
(
	id       string comment '编号', 
	tm_name  string comment '品牌名称', 
	logo_url string comment '品牌logo的图片路径'
) comment '品牌表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_base_trademark_full/';


-- -------------------------------------------------------------------------------------------------
-- 购物车表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_cart_info_full;
create external table  if not exists ods_cart_info_full
(
	id           string         comment '编号',
	user_id      string         comment '用户id',
	sku_id       string         comment 'sku_id',
	cart_price   decimal(16, 2) comment '放入购物车时价格',
	sku_num      bigint         comment '数量',
	img_url      bigint         comment '商品图片地址',
	sku_name     string         comment 'sku名称 (冗余)',
	is_checked   string         comment '是否被选中',
	create_time  string         comment '创建时间',
	operate_time string         comment '修改时间',
	is_ordered   string         comment '是否已经下单',
	order_time   string         comment '下单时间',
	source_type  string         comment '来源类型',
	source_id    string         comment '来源编号'
) comment '购物车全量表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_cart_info_full/';


-- -------------------------------------------------------------------------------------------------
-- 优惠券信息表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_coupon_info_full;
create external table  if not exists ods_coupon_info_full
(
	id               string         comment '购物券编号',
	coupon_name      string         comment '购物券名称',
	coupon_type      string         comment '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
	condition_amount decimal(16, 2) comment '满额数',
	condition_num    bigint         comment '满件数',
	activity_id      string         comment '活动编号',
	benefit_amount   decimal(16, 2) comment '减金额',
	benefit_discount decimal(16, 2) comment '折扣',
	create_time      string         comment '创建时间',
	range_type       string         comment '范围类型 1、商品 2、品类 3、品牌',
	limit_num        bigint         comment '最多领用次数',
	taken_count      bigint         comment '已领用次数',
	start_time       string         comment '开始领取时间',
	end_time         string         comment '结束领取时间',
	operate_time     string         comment '修改时间',
	expire_time      string         comment '过期时间'
) comment '优惠券信息表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_coupon_info_full/';


-- -------------------------------------------------------------------------------------------------
-- 商品平台属性表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_sku_attr_value_full;
create external table  if not exists ods_sku_attr_value_full
(
	id         string comment '编号',
	attr_id    string comment '平台属性ID',
	value_id   string comment '平台属性值ID',
	sku_id     string comment '商品ID',
	attr_name  string comment '平台属性名称',
	value_name string comment '平台属性值名称'
) comment 'sku平台属性表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_attr_value_full/';


-- -------------------------------------------------------------------------------------------------
-- 商品表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_sku_info_full;
create external table  if not exists ods_sku_info_full
(
	id              string         comment 'skuId',
	spu_id          string         comment 'spuid',
	price           decimal(16, 2) comment '价格',
	sku_name        string         comment '商品名称',
	sku_desc        string         comment '商品描述',
	weight          decimal(16, 2) comment '重量',
	tm_id           string         comment '品牌id',
	category3_id    string         comment '品类id',
	sku_default_igm string         comment '商品图片地址',
	is_sale         string         comment '是否在售',
	create_time     string         comment '创建时间'
) comment 'SKU商品表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_info_full/';


-- -------------------------------------------------------------------------------------------------
-- 商品销售属性值表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_sku_sale_attr_value_full;
create external table  if not exists ods_sku_sale_attr_value_full
(
	id                   string comment '编号',
	sku_id               string comment 'sku_id',
	spu_id               string comment 'spu_id',
	sale_attr_value_id   string comment '销售属性值id',
	sale_attr_id         string comment '销售属性id',
	sale_attr_name       string comment '销售属性名称',
	sale_attr_value_name string comment '销售属性值名称'
) comment 'sku销售属性名称' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_sku_sale_attr_value_full/';


-- -------------------------------------------------------------------------------------------------
--  SPU 表（全量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_spu_info_full;
create external table  if not exists ods_spu_info_full
(
	id           string comment 'spu_id',
	spu_name     string comment 'spu名称',
	description  string comment '描述信息',
	category3_id string comment '品类id',
	tm_id        string comment '品牌id'
) comment 'SPU商品表' partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '' 
    location '/warehouse/ods/ods_spu_info_full/';


-- -------------------------------------------------------------------------------------------------
-- 购物车表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_cart_info_inc;
create external table  if not exists ods_cart_info_inc
(
	type string                                                                                            comment '变动类型',
	ts   bigint                                                                                            comment '变动时间',
	data struct<id: string,          user_id: string,      sku_id: string,     cart_price: decimal(16, 2),
	           sku_num :bigint,      img_url: string,      sku_name: string,   is_checked: string,  
	           create_time: string,  operate_time: string, is_ordered: string, order_time: string,  
	           source_type: string,  source_id: string>                                                    comment '数据',
	old  map<string, string>                                                                               comment '旧值'
) comment '购物车增量表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_cart_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 评论表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_comment_info_inc;
create external table  if not exists ods_comment_info_inc
(
	type string                                                                              comment '变动类型',
	ts   bigint                                                                              comment '变动时间',
	data struct<id: string,          user_id: string,      nick_name: string,  head_img: string,  
	            sku_id: string,      spu_id: string,       order_id: string,   appraise: string,  
	            comment_txt: string, create_time: string, operate_time :string>             comment '数据',
	old  map<string, string>                                                                 comment '旧值'
) comment '评价表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_comment_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 优惠券领用表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_coupon_use_inc;
create external table  if not exists ods_coupon_use_inc
(
	type string comment '变动类型',
	ts   bigint comment '变动时间',
	data struct<id: string,         coupon_id: string,     user_id: string,  
	            order_id: string,   coupon_status: string, get_time: string,  
	            using_time: string, used_time: string,     expire_time :string> comment '数据',
	old  map<string, string>                                                     comment '旧值'
) comment '优惠券领用表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_coupon_use_inc/';


-- -------------------------------------------------------------------------------------------------
-- 收藏表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_favor_info_inc;
create external table  if not exists ods_favor_info_inc
(
	type string                                                                             comment '变动类型',
	ts   bigint                                                                             comment '变动时间',
	data struct<id: string,        user_id: string,     sku_id: string,  spu_id: string,  
	            is_cancel: string, create_time: string, cancel_time: string>                comment '数据',
	old  map<string, string>                                                                comment '旧值'
) comment '收藏表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_favor_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 订单明细表（增量表）
-- -------------------------------------------------------------------------------------------------
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
) comment '订单明细表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_detail_inc/';


-- -------------------------------------------------------------------------------------------------
-- 订单明细活动关联表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_detail_activity_inc;
create external table  if not exists ods_order_detail_activity_inc
(
	type string                                                                                             comment '变动类型',
	ts   bigint                                                                                             comment '变动时间',
	data struct<id: string,               order_id: string,  order_detail_id: string,  activity_id: string,  
	            activity_rule_id: string, sku_id: string,    create_time : string>                          comment '数据',
	old  map<string, string>                                                                                comment '旧值'
) comment '订单明细活动关联表' partitioned by (dt string) row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' location '/warehouse/ods/ods_order_detail_activity_inc/';


-- -------------------------------------------------------------------------------------------------
-- 订单明细优惠券关联表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_detail_coupon_inc;
create external table  if not exists ods_order_detail_coupon_inc
(
	type string                                                                                       comment '变动类型',
	ts   bigint                                                                                       comment '变动时间',
	data struct<id: string,            order_id: string, order_detail_id: string,  coupon_id: string,  
	            coupon_use_id: string, sku_id: string,   create_time: string>                         comment '数据',
	old  map<string, string>                                                                          comment '旧值'
) comment '订单明细优惠券关联表' partitioned by (dt string) row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' location '/warehouse/ods/ods_order_detail_coupon_inc/';


-- -------------------------------------------------------------------------------------------------
-- 订单表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_info_inc;
create external table  if not exists ods_order_info_inc
(
	type string                                                                                                                    comment '变动类型',
	ts   bigint                                                                                                                    comment '变动时间',
	data struct<id: string,                            consignee: string,                     consignee_tel: string,  
	            total_amount: decimal(16, 2),          order_status: string,                  user_id: string,  
	            payment_way: string,                   delivery_address: string,              order_comment: string,  
	            out_trade_no: string,                  trade_body: string,                    create_time: string,  
	            operate_time: string,                  expire_time: string,                   process_status: string,  
	            tracking_no: string,                   parent_order_id: string,               img_url: string,  
	            province_id: string,                   activity_reduce_amount:decimal(16, 2), coupon_reduce_amount: decimal(16, 2),
	            original_total_amount :decimal(16, 2), freight_fee: decimal(16, 2),           freight_fee_reduce: decimal(16, 2),
	            refundable_time: decimal(16, 2)>                                                                                   comment '数据',
	old  map<string, string>                                                                                                       comment '旧值'
) comment '订单表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 退单表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_refund_info_inc;
create external table  if not exists ods_order_refund_info_inc
(
	type string                                                                                       comment '变动类型',
	ts   bigint                                                                                       comment '变动时间',
	data struct<id: string,                    user_id: string,            order_id: string,  
	            sku_id: string,                refund_type: string,        refund_num :bigint,
	            refund_amount: decimal(16, 2), refund_reason_type: string, refund_reason_txt: string,  
	            refund_status: string,         create_time:string>                                    comment '数据',
	old  map<string, string>                                                                          comment '旧值'
) comment '退单表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_refund_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 订单状态流水表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_status_log_inc;
create external table  if not exists ods_order_status_log_inc
(
	type string                                                                           comment '变动类型',
	ts   bigint                                                                           comment '变动时间',
	data struct<id: string, order_id: string, order_status: string, operate_time: string> comment '数据',
	old  map<string, string>                                                              comment '旧值'
) comment '退单表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_order_status_log_inc/';


-- -------------------------------------------------------------------------------------------------
-- 支付表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_payment_info_inc;
create external table  if not exists ods_payment_info_inc
(
	type string                                                                               comment '变动类型',
	ts   bigint                                                                               comment '变动时间',
	data struct<id: string,                   out_trade_no: string,  order_id: string,  
	            user_id: string,              payment_type: string,  trade_no: string,  
	            total_amount: decimal(16, 2), subject: string,       payment_status: string,  
	            create_time: string,          callback_time: string, callback_content: string> comment '数据',
	old  map<string, string>                                                                   comment '旧值'
) comment '支付表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_payment_info_inc/';


-- -------------------------------------------------------------------------------------------------
-- 退款表（增量表）
-- -------------------------------------------------------------------------------------------------
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
) comment '退款表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_refund_payment_inc/';


-- -------------------------------------------------------------------------------------------------
-- 用户表（增量表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_user_info_inc;
create external table  if not exists ods_user_info_inc
(
	type string                                                             comment '变动类型',
	ts   bigint                                                             comment '变动时间',
	data struct<id: string,       login_name: string,  nick_name: string,  
	            passwd: string,   name: string,        phone_num: string,  
	            email: string,    head_img: string,    user_level: string,  
	            birthday: string, gender: string,      create_time: string,  
	            operate_time: string,  status :string>                      comment '数据',
	old  map<string, string>                                                comment '旧值'
) comment '用户表' partitioned by (dt string) 
    row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe' 
    location '/warehouse/ods/ods_user_info_inc/';

-- 业务表数据装载脚本：hdfs_ods_db.sh all 2021-08-15

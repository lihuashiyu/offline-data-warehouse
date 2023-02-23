-- -------------------------------------------------------------------------------------------------
-- 初始化数据库
-- 
-- drop database if exists at_gui_gu;
-- create database if not exists at_gui_gu;
-- use at_gui_gu;
-- show tables;
-- -------------------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------------------
-- 各活动补贴率
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_activity_stats;
create table if not exists ads_activity_stats
(
	dt            date        not null comment '统计日期',
	activity_id   varchar(16) not null comment '活动 ID',
	activity_name varchar(64)          comment '活动名称',
	start_date    varchar(16)          comment '活动开始日期',
	reduce_rate   decimal(16, 2)       comment '补贴率',
	primary key (dt, activity_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '活动统计';

-- -------------------------------------------------------------------------------------------------
-- 各优惠券补贴率
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_coupon_stats;
create table if not exists ads_coupon_stats
(
	dt          date        not null comment '统计日期',
	coupon_id   varchar(16) not null comment '优惠券 ID',
	coupon_name varchar(64)          comment '优惠券名称',
	start_date  varchar(16)          comment '发布日期',
	rule_name   varchar(64)          comment '优惠规则，例如满 100 元减 10 元',
	reduce_rate decimal(16, 2)       comment '补贴率',
	primary key (dt, coupon_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '优惠券统计';

-- -------------------------------------------------------------------------------------------------
-- 新增交易用户统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_new_buyer_stats;
create table if not exists ads_new_buyer_stats
(
	dt                     date       not null comment '统计日期',
	recent_days            bigint(20) not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	new_order_user_count   bigint(20)          comment '新增下单人数',
	new_payment_user_count bigint(20)          comment '新增支付人数',
	primary key (dt, recent_days) using btree
) engine = InnoDB character set = utf8mb4 comment = '新增交易用户统计';

-- -------------------------------------------------------------------------------------------------
-- 各省份订单统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_order_by_province;
create table if not exists ads_order_by_province
(
	dt                 date        not null comment '统计日期',
	recent_days        bigint(20)  not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	province_id        varchar(16) not null comment '省份 ID',
	province_name      varchar(16)          comment '省份名称',
	area_code          varchar(16)          comment '地区编码',
	iso_code           varchar(16)          comment '国际标准地区编码',
	iso_code_3166_2    varchar(16)          comment '国际标准地区编码',
	order_count        bigint(20)           comment '订单数',
	order_total_amount decimal(16, 2)       comment '订单金额',
	primary key (dt, recent_days, province_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '各地区订单统计';

-- -------------------------------------------------------------------------------------------------
-- 用户路径分析
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_page_path;
create table if not exists ads_page_path
(
	dt          date        not null comment '统计日期',
	recent_days bigint(20)  not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	source      varchar(64) not null comment '跳转起始页面 ID',
	target      varchar(64) not null comment '跳转终到页面 ID',
	path_count  bigint(20)           comment '跳转次数',
	primary key (dt, recent_days, source, target) using btree
) engine = InnoDB character set = utf8mb4 comment = '页面浏览路径分析';

-- -------------------------------------------------------------------------------------------------
-- 各品牌复购率
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_repeat_purchase_by_tm;
create table if not exists ads_repeat_purchase_by_tm
(
	dt                date        not null comment '统计日期',
	recent_days       bigint(20)  not null comment '最近天数,7:最近7天,30:最近30天',
	tm_id             varchar(16) not null comment '品牌 ID',
	tm_name           varchar(32)          comment '品牌名称',
	order_repeat_rate decimal(16, 2)       comment '复购率',
	primary key (dt, recent_days, tm_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '各品牌复购率统计';

-- -------------------------------------------------------------------------------------------------
-- 各品类商品购物车存量topN
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_sku_cart_num_top3_by_cate;
create table if not exists ads_sku_cart_num_top3_by_cate
(
	dt             date        not null comment '统计日期',
	category1_id   varchar(16) not null comment '一级分类 ID',
	category1_name varchar(64)          comment '一级分类名称',
	category2_id   varchar(16) not null comment '二级分类 ID',
	category2_name varchar(64)          comment '二级分类名称',
	category3_id   varchar(16) not null comment '三级分类 ID',
	category3_name varchar(64)          comment '三级分类名称',
	sku_id         varchar(16) not null comment '商品 ID',
	sku_name       varchar(128)         comment '商品名称',
	cart_num       bigint(20)           comment '购物车中商品数量',
	rk             bigint(20)           comment '排名',
	primary key (dt, sku_id, category1_id, category2_id, category3_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '各分类商品购物车存量 Top10';

-- -------------------------------------------------------------------------------------------------
-- 交易综合统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_trade_stats;
create table if not exists ads_trade_stats
(
	dt                      date        not null comment '统计日期',
	recent_days             bigint(255) not null comment '最近天数：1，最近 1 日；7，最近 7 天；30，最近 30 天',
	order_total_amount      decimal(16, 2)       comment '订单总额，GMV',
	order_count             bigint(20)           comment '订单数',
	order_user_count        bigint(20)           comment '下单人数',
	order_refund_count      bigint(20)           comment '退单数',
	order_refund_user_count bigint(20)           comment '退单人数',
	primary key (dt, recent_days) using btree
) engine = InnoDB character set = utf8mb4 comment = '交易统计';

-- -------------------------------------------------------------------------------------------------
-- 各品类商品交易统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_trade_stats_by_cate;
create table if not exists ads_trade_stats_by_cate
(
	dt                      date        not null comment '统计日期',
	recent_days             bigint(20)  not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	category1_id            varchar(16) not null comment '一级分类 ID',
	category1_name          varchar(64)          comment '一级分类名称',
	category2_id            varchar(16) not null comment '二级分类 ID',
	category2_name          varchar(64)          comment '二级分类名称',
	category3_id            varchar(16) not null comment '三级分类 ID',
	category3_name          varchar(64)          comment '三级分类名称',
	order_count             bigint(20)           comment '订单数',
	order_user_count        bigint(20)           comment '订单人数',
	order_refund_count      bigint(20)           comment '退单数',
	order_refund_user_count bigint(20)           comment '退单人数',
	primary key (dt, recent_days, category1_id, category2_id, category3_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '各分类商品交易统计';

-- -------------------------------------------------------------------------------------------------
-- 各品牌商品交易统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_trade_stats_by_tm;
create table if not exists ads_trade_stats_by_tm
(
	dt                      date        not null comment '统计日期',
	recent_days             bigint(20)  not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	tm_id                   varchar(16) not null comment '品牌 ID',
	tm_name                 varchar(32)          comment '品牌名称',
	order_count             bigint(20)           comment '订单数',
	order_user_count        bigint(20)           comment '订单人数',
	order_refund_count      bigint(20)           comment '退单数',
	order_refund_user_count bigint(20)           comment '退单人数',
	primary key (dt, recent_days, tm_id) using btree
) engine = InnoDB character set = utf8mb4 comment = '各品牌商品交易统计';

-- -------------------------------------------------------------------------------------------------
-- 各渠道流量统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_traffic_stats_by_channel;
create table if not exists ads_traffic_stats_by_channel
(
	dt               date        not null comment '统计日期',
	recent_days      bigint(20)  not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	channel          varchar(16) not null comment '渠道',
	uv_count         bigint(20)           comment '访客人数',
	avg_duration_sec bigint(20)           comment '会话平均停留时长，单位为秒',
	avg_page_count   bigint(20)           comment '会话平均浏览页面数',
	sv_count         bigint(20)           comment '会话数',
	bounce_rate      decimal(16, 2)       comment '跳出率',
	primary key (dt, recent_days, channel) using btree
) engine = InnoDB character set = utf8mb4 comment = '各渠道流量统计';

-- -------------------------------------------------------------------------------------------------
-- 用户行为漏斗分析
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_user_action;
create table if not exists ads_user_action
(
	dt                date       not null comment '统计日期',
	recent_days       bigint(20) not null comment '最近天数：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	home_count        bigint(20)          comment '浏览首页人数',
	good_detail_count bigint(20)          comment '浏览商品详情页人数',
	cart_count        bigint(20)          comment '加入购物车人数',
	order_count       bigint(20)          comment '下单人数',
	payment_count     bigint(20)          comment '支付人数',
	primary key (dt, recent_days) using btree
) engine = InnoDB character set = utf8mb4 comment = '漏斗分析';

-- -------------------------------------------------------------------------------------------------
-- 用户变动统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_user_change;
create table if not exists ads_user_change
(
	dt               varchar(16) primary key comment '统计日期',
	user_churn_count varchar(16)             comment '流失用户数',
	user_back_count  varchar(16)             comment '回流用户数'
) engine = InnoDB character set = utf8mb4 comment = '用户变动统计';

-- -------------------------------------------------------------------------------------------------
-- 用户留存率
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_user_retention;
create table if not exists ads_user_retention
(
	dt              date        not null comment '统计日期',
	create_date     varchar(16) not null comment '用户新增日期',
	retention_day   int(20)     not null comment '截至当前日期留存天数',
	retention_count bigint(20)           comment '留存用户数量',
	new_user_count  bigint(20)           comment '新增用户数量',
	retention_rate  decimal(16, 2)       comment '留存率',
	primary key (dt, create_date, retention_day) using btree
) engine = InnoDB character set = utf8mb4 comment = '留存率';

-- -------------------------------------------------------------------------------------------------
-- 用户新增活跃统计
-- -------------------------------------------------------------------------------------------------
drop table if exists ads_user_stats;
create table if not exists ads_user_stats
(
	dt                date       not null comment '统计日期',
	recent_days       bigint(20) not null comment '最近 N 日：1，最近 1 日；7，最近 7 日；30，最近 30 日',
	new_user_count    bigint(20)          comment '新增用户数',
	active_user_count bigint(20)          comment '活跃用户数',
	primary key (dt, recent_days) using btree
) engine = InnoDB character set = utf8mb4 comment = '用户新增活跃统计';

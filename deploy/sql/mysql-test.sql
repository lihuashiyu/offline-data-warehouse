-- select { * | <字段列名> }
-- from   < 表 1>, <表 2 > … 
-- where  < 表达式 >
-- group by < group by definition >
-- having < expression > [ { <operator> <expression> } … ]
-- order by < order by definition >
-- limit [<offset>,] <row count>];

select * from information_schema.views;                              -- 数据库中的视图的信息
select * from information_schema.schemata;                           -- 所有数据库的信息
select * from information_schema.tables;                             -- 表的信息（包括视图）
select * from information_schema.columns;                            -- 表中的列信息
select * from information_schema.statistics;                         -- 表索引的信息
select * from information_schema.table_constraints;                  -- 表的约束类型
select * from information_schema.key_column_usage;                   -- 具有约束的键列
select * from information_schema.routines;                           -- 关于存储子程序（存储程序和函数）的信息
select * from information_schema.triggers;                           -- 关于触发程序的信息
select * from information_schema.character_sets;                     -- mysql实例可用字符集的信息

-- 数据库中所有表的信息
select table_name,
       table_type,
       table_rows,
       avg_row_length,
       data_length,
       create_time,
       update_time,
       table_comment
from information_schema.tables
where table_schema = 'at_gui_gu';


-- 数据库中每个表的数据量
select table_name, table_rows from information_schema.tables where table_schema = 'at_gui_gu' order by table_rows desc;
select table_name, table_rows from information_schema.tables where table_schema = 'at_gui_gu' and table_rows > 0 order by table_name;



select id,
       login_name,
       nick_name,
       passwd,
       name,
       phone_num,
       email,
       head_img,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time
from user_info
where id in (?);

delete from  user_info where id in (101, 102, 103, 104);

-- SHOW VARIABLES LIKE 'log_bin';
-- show global variables like 'binlog%';
-- show tables;

select * from cart_info order by id limit 1;
select * from cart_info order by id desc;
select * from cart_info order by id;

select create_time from cart_info group by create_time;

show tables ;

select * from activity_info;

select column_name from information_schema.columns where table_schema = 'at_gui_gu'
           and table_name = 'activity_info' order by ordinal_position;


INSERT INTO at_gui_gu.activity_info (id, activity_name, activity_type, activity_desc, start_time, end_time, create_time) VALUES (5, '女神节', '3102', '满件打折', '2022-08-02 14:24:38', '2022-08-02 14:24:38', null);


select count(*) from cart_info;
select count(*) from comment_info;
select count(*) from coupon_use;
select count(*) from favor_info;
select count(*) from order_detail;
select count(*) from order_detail_activity;
select count(*) from order_detail_coupon;
select count(*) from order_info;
select count(*) from order_refund_info;
select count(*) from order_status_log;
select count(*) from payment_info;
select count(*) from refund_payment;
select count(*) from user_info;

select * from ads_activity_stats;

select * from base_dic;



select d.NAME,t.TBL_NAME,t.TBL_ID,p.PART_ID,p.PART_NAME,a.PARAM_VALUE
	from hive.TBLS t
		     left join hive.DBS d
		               on t.DB_ID = d.DB_ID
		     left join hive.PARTITIONS p
		               on t.TBL_ID = p.TBL_ID
		     left join hive.PARTITION_PARAMS a
		               on p.PART_ID=a.PART_ID
	where t.TBL_NAME='tracklog' and d.NAME='ods' and a.PARAM_KEY='numRows';  
 
show tables;

select * from ads_activity_stats;
select * from ads_coupon_stats;
select * from ads_new_buyer_stats;               -- 空
select * from ads_order_by_province;
select * from ads_page_path;
select * from ads_repeat_purchase_by_tm;
select * from ads_sku_cart_num_top3_by_cate;
select * from ads_trade_stats;
select * from ads_trade_stats_by_cate;
select * from ads_trade_stats_by_tm;
select * from ads_traffic_stats_by_channel;
select * from ads_user_action;
select * from ads_user_change;
select * from ads_user_retention;
select * from ads_user_stats;

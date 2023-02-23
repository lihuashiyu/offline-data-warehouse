-- -------------------------------------------------------------------------------------------------
-- 初始
-- -------------------------------------------------------------------------------------------------
show databases ;
use warehouse;
show tables ;


-- -------------------------------------------------------------------------------------------------
-- 测试
-- -------------------------------------------------------------------------------------------------
select * from student;
insert into student values (1, '张三', 33, 1, 172.1, 48.9, 'zhangsan@qq.com', '学生');
insert into student values (2, '李四', 23, 0, 165.1, 53.9, 'lisi@qq.com', '学生');


-- -------------------------------------------------------------------------------------------------
-- ODS
-- -------------------------------------------------------------------------------------------------
show tables;

select * from ods_activity_info_full limit 1;
select * from ods_activity_rule_full limit 1;
select * from ods_base_category1_full limit 1;
select * from ods_base_category2_full limit 1;
select * from ods_base_category3_full limit 1;
select * from ods_base_dic_full limit 1;
select * from ods_base_province_full limit 1;
select * from ods_base_region_full limit 1;
select * from ods_base_trademark_full limit 1;
select * from ods_cart_info_full limit 1;
select * from ods_cart_info_inc limit 1;
select * from ods_comment_info_inc limit 1;
select * from ods_coupon_info_full limit 1;
select * from ods_coupon_use_inc limit 1;
select * from ods_favor_info_inc limit 1;
select * from ods_log_inc limit 1;
select * from ods_order_detail_activity_inc limit 1;
select * from ods_order_detail_coupon_inc limit 1;
select * from ods_order_detail_inc limit 1;
select * from ods_order_info_inc limit 1;
select * from ods_order_refund_info_inc limit 1;
select * from ods_order_status_log_inc limit 1;
select * from ods_payment_info_inc limit 1;
select * from ods_refund_payment_inc limit 1;
select * from ods_sku_attr_value_full limit 1;
select * from ods_sku_info_full limit 1;
select * from ods_sku_sale_attr_value_full limit 1;
select * from ods_spu_info_full limit 1;
select * from ods_user_info_inc limit 1;

-- -------------------------------------------------------------------------------------------------
-- DIM
-- -------------------------------------------------------------------------------------------------
show tables like 'dim_*';
select * from dim_activity_full limit 1;
select * from dim_coupon_full limit 1;
select * from dim_date limit 1;
select * from dim_province_full limit 1;
select * from dim_sku_full limit 1;
select * from dim_user_zip limit 1;

select * from dim_activity_full;
select * from dim_coupon_full;
select * from dim_date;
select * from dim_province_full;
select * from dim_sku_full;
select * from dim_user_zip;


-- -------------------------------------------------------------------------------------------------
-- DWD
-- -------------------------------------------------------------------------------------------------
select * from dwd_interaction_comment_inc;
select * from dwd_interaction_favor_add_inc;
select * from dwd_tool_coupon_get_inc;
select * from dwd_tool_coupon_order_inc;
select * from dwd_tool_coupon_pay_inc;
select * from dwd_trade_cancel_detail_inc;      -- 空
select * from dwd_trade_cart_add_inc;
select * from dwd_trade_cart_full;
select * from dwd_trade_order_detail_inc;
select * from dwd_trade_order_refund_inc;
select * from dwd_trade_pay_detail_suc_inc;      -- 空
select * from dwd_trade_refund_pay_suc_inc;      -- 空
select * from dwd_traffic_action_inc;
select * from dwd_traffic_display_inc;
select * from dwd_traffic_error_inc;
select * from dwd_traffic_page_view_inc;
select * from dwd_traffic_start_inc;
select * from dwd_user_login_inc;
select * from dwd_user_register_inc;


-- -------------------------------------------------------------------------------------------------
-- DWS
-- -------------------------------------------------------------------------------------------------
select * from dws_trade_activity_order_nd;
select * from dws_trade_coupon_order_nd;
select * from dws_trade_province_order_1d;
select * from dws_trade_province_order_nd;
select * from dws_trade_user_cart_add_1d;
select * from dws_trade_user_cart_add_nd;
select * from dws_trade_user_order_1d;
select * from dws_trade_user_order_nd;
select * from dws_trade_user_order_refund_1d;
select * from dws_trade_user_order_refund_nd;
select * from dws_trade_user_order_td;
select * from dws_trade_user_payment_1d;         -- 空
select * from dws_trade_user_payment_nd;         -- 空
select * from dws_trade_user_payment_td;         -- 空
select * from dws_trade_user_sku_order_1d;
select * from dws_trade_user_sku_order_nd;
select * from dws_trade_user_sku_order_refund_1d;
select * from dws_trade_user_sku_order_refund_nd;
select * from dws_traffic_page_visitor_page_view_1d;
select * from dws_traffic_page_visitor_page_view_nd;
select * from dws_traffic_session_page_view_1d;
select * from dws_user_user_login_td;


-- -------------------------------------------------------------------------------------------------
-- ADS
-- -------------------------------------------------------------------------------------------------
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


select d.name, t.tbl_name, t.tbl_id, p.part_id, p.part_name, a.param_value
	from tbls t
		     left join dbs d on t.db_id = d.db_id
		     left join partitions p on t.tbl_id = p.tbl_id
		     left join partition_params a on p.part_id = a.part_id
	where t.tbl_name = 'tracklog'
	  and d.name = 'ods'
	  and a.param_key = 'numRows';  
  
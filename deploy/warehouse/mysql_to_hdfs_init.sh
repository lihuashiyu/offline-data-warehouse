#!/usr/bin/env bash
    
    
APP=gmall
MYSQL_URL="jdbc:mysql://issac:3306"                        # Mysql 连接 URL
MYSQL_USER=issac                                           # Mysql 用户名
MYSQL_PASSWD=111111                                        # Mysql 密码
WARE_HOUSE_DIR=/user/warehouse/                            # 仓库在 HDFS 上的路径
SQOOP=/opt/apache/sqoop/bin/sqoop                          # Sqoop 安装路径
    
    
if [ -n "$2" ] ;then
    do_date=$2
else 
    echo " ****** 请传入日期参数 ****** "
    exit
fi 
    
        
import_data()
{
    ${SQOOP} import \
             --connect ${MYSQL_URL}/${APP} \
             --username ${MYSQL_USER} \
             --password ${MYSQL_PASSWD} \
             --target-dir ${WARE_HOUSE_DIR}/${APP}/db/$1/${do_date} \
             --delete-target-dir \
             --query "$2 where \$CONDITIONS" \
             --num-mappers 1 \
             --fields-terminated-by '\t' \
             # --compress \
             # --compression-codec lzop \
             --null-string '\\N' \
             --null-non-string '\\N'

    # hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar \ 
    #     com.hadoop.compression.lzo.DistributedLzoIndexer \
    #     /origin_data/$APP/db/$1/$do_date
}

import_order_info()
{
    import_data order_info "select id, 
                                   total_amount, 
                                   order_status, 
                                   user_id, 
                                   payment_way,
                                   delivery_address,
                                   out_trade_no, 
                                   create_time, 
                                   operate_time,
                                   expire_time,
                                   tracking_no,
                                   province_id,
                                   activity_reduce_amount,
                                   coupon_reduce_amount,                            
                                   original_total_amount,
                                   feight_fee,
                                   feight_fee_reduce      
                            from order_info"
}

import_coupon_use()
{
    import_data coupon_use "select id,
                                   coupon_id,
                                   user_id,
                                   order_id,
                                   coupon_status,
                                   get_time,
                                   using_time,
                                   used_time,
                                   expire_time
                            from coupon_use"
}

import_order_status_log()
{
    import_data order_status_log "select id,
                                         order_id,
                                         order_status,
                                         operate_time
                                  from order_status_log"
}

import_user_info()
{
    import_data "user_info" "select id,
                                    login_name,
                                    nick_name,
        ) old full outer join
        (
            select id,
                   login_name,
                   nick_name,
                   md5(name) name,
                   md5(phone_num) phone_num,
                   md5(email) email,
                   user_level,
                   birthday,
                   gender,
                   create_time,
                   operate_time,
                   name,
                   phone_num,
                   email,
                   user_level, 
                   birthday,
                   gender,
                   create_time,
                   operate_time
            from user_info"
}

import_order_detail()
{
    import_data order_detail "select id,
                                     order_id, 
                                     sku_id,
                                     sku_name,
                                     order_price,
                                     sku_num, 
                                     create_time,
                                     source_type,
                                     source_id,
                                     split_total_amount,
                                     split_activity_amount,
                                     split_coupon_amount
                              from order_detail"
}

import_payment_info()
{
    import_data payment_info  "select id,  
                                      out_trade_no, 
                                      order_id, 
                                      user_id, 
                                      payment_type, 
                                      trade_no, 
                                      total_amount,  
                                      subject, 
                                      payment_status,
                                      create_time,
                                      callback_time 
                               from payment_info"
}

import_comment_info()
{
    import_data comment_info "select id,
                                     user_id,
                                     sku_id,
                                     spu_id,
                                     order_id,
                                     appraise,
                                     create_time
                              from comment_info"
}

import_order_refund_info()
{
    import_data order_refund_info "select id,
                                          user_id,
                                          order_id,
                                          sku_id,
                                          refund_type,
                                          refund_num,
                                          refund_amount,
                                          refund_reason_type,
                                          refund_status,
                                          create_time
                                   from order_refund_info"
}

import_sku_info()
{
    import_data sku_info "select id,
                                 spu_id,
                                 price,
                                 sku_name,
                                 sku_desc,
                                 weight,
                                 tm_id,
                                 category3_id,
                                 is_sale,
                                 create_time
                          from sku_info"
}

import_base_category1()
{
    import_data base_category1 "select id, name from base_category1"
}

import_base_category2()
{
    import_data base_category2 "select id, name, category1_id from base_category2"
}

import_base_category3()
{
    import_data base_category3 "select id, name, category2_id from base_category3"
}

import_base_province()
{
    import_data base_province "select id,
                                      name,
                                      region_id,
                                      area_code,
                                      iso_code,
                                      iso_3166_2
                               from base_province"
}

import_base_region()
{
    import_data base_region "select id, region_name from base_region"
}

import_base_trademark()
{
    import_data base_trademark "select id, tm_name from base_trademark"
}

import_spu_info()
{
    import_data spu_info "select id,
                                 spu_name,
                                 category3_id,
                                 tm_id
                          from spu_info"
}

import_favor_info()
{
    import_data favor_info "select id,
                                   user_id,
                                   sku_id,
                                   spu_id,
                                   is_cancel,
                                   create_time,
                                   cancel_time
                            from favor_info"
}

import_cart_info()
{
    import_data cart_info "select id,
                                  user_id,
                                  sku_id,
                                  cart_price,
                                  sku_num,
                                  sku_name,
                                  create_time,
                                  operate_time,
                                  is_ordered,
                                  order_time,
                                  source_type,
                                  source_id
                           from cart_info"
}

import_coupon_info()
{
    import_data coupon_info "select id,
                                    coupon_name,
                                    coupon_type,
                                    condition_amount,
                                    condition_num,
                                    activity_id,
                                    benefit_amount,
                                    benefit_discount,
                                    create_time,
                                    range_type,
                                    limit_num,
                                    taken_count,
                                    start_time,
                                    end_time,
                                    operate_time,
                                    expire_time
                             from coupon_info"
}

import_activity_info()
{
    import_data activity_info "select id,
                                      activity_name,
                                      activity_type,
                                      start_time,
                                      end_time,
                                      create_time
                               from activity_info"
}

import_activity_rule()
{
    import_data activity_rule "select id,
                                      activity_id,
                                      activity_type,
                                      condition_amount,
                                      condition_num,
                                      benefit_amount,
                                      benefit_discount,
                                      benefit_level
                               from activity_rule"
}

import_base_dic()
{
    import_data base_dic "select dic_code,
                                 dic_name,
                                 parent_code,
                                 create_time,
                                 operate_time
                          from base_dic"
}


import_order_detail_activity()
{
    import_data order_detail_activity "select id,
                                              order_id,
                                              order_detail_id,
                                              activity_id,
                                              activity_rule_id,
                                              sku_id,
                                              create_time
                                       from order_detail_activity"
}


import_order_detail_coupon()
{
    import_data order_detail_coupon "select id,
                                            order_id,
                                            order_detail_id,
                                            coupon_id,
                                            coupon_use_id,
                                            sku_id,
                                            create_time
                                     from order_detail_coupon"
}


import_refund_payment()
{
    import_data refund_payment "select id,
                                       out_trade_no,
                                       order_id,
                                       sku_id,
                                       payment_type,
                                       trade_no,
                                       total_amount,
                                       subject,
                                       refund_status,
                                       create_time,
                                       callback_time
                                from refund_payment"
}

import_sku_attr_value()
{
    import_data sku_attr_value "select id,
                                       attr_id,
                                       value_id,
                                       sku_id,
                                       attr_name,
                                       value_name
                                from sku_attr_value"
}


import_sku_sale_attr_value()
{
    import_data sku_sale_attr_value "select id,
                                            sku_id,
                                            spu_id,
                                            sale_attr_value_id,
                                            sale_attr_id,
                                            sale_attr_name,
                                            sale_attr_value_name
                                     from sku_sale_attr_value"
}

case $1 in
    "order_info")
        import_order_info
    ;;
    "base_category1")
        import_base_category1
    ;;
    "base_category2")
        import_base_category2
    ;;
    "base_category3")
        import_base_category3
    ;;
    "order_detail")
         import_order_detail
    ;;
    "sku_info")
        import_sku_info
    ;;
    "user_info")
        import_user_info
    ;;
    "payment_info")
        import_payment_info
    ;;
    "base_province")
        import_base_province
    ;;
    "base_region")
        import_base_region
    ;;
     "base_trademark")
        import_base_trademark
    ;;
    "activity_info")
        import_activity_info
    ;;
    "cart_info")
        import_cart_info
    ;;
    "comment_info")
        import_comment_info
    ;;
    "coupon_info")
        import_coupon_info
    ;;
    "coupon_use")
        import_coupon_use
    ;;
    "favor_info")
        import_favor_info
    ;;
    "order_refund_info")
        import_order_refund_info
    ;;
    "order_status_log")
        import_order_status_log
    ;;
    "spu_info")
        import_spu_info
    ;;
    "activity_rule")
        import_activity_rule
    ;;
    "base_dic")
        import_base_dic
    ;;
    "order_detail_activity")
        import_order_detail_activity
    ;;
    "order_detail_coupon")
        import_order_detail_coupon
    ;;
    "refund_payment")
        import_refund_payment
    ;;
    "sku_attr_value")
        import_sku_attr_value
    ;;
    "sku_sale_attr_value")
        import_sku_sale_attr_value
    ;;
    "all")
        import_base_category1
        import_base_category2
        import_base_category3
        import_order_info
        import_order_detail
        import_sku_info
        import_user_info
        import_payment_info
        import_base_region
        import_base_province
        import_base_trademark
        import_activity_info
        import_cart_info
        import_comment_info
        import_coupon_use
        import_coupon_info
        import_favor_info
        import_order_refund_info
        import_order_status_log
        import_spu_info
        import_activity_rule
        import_base_dic
        import_order_detail_activity
        import_order_detail_coupon
        import_refund_payment
        import_sku_attr_value
        import_sku_sale_attr_value
    ;;
esac

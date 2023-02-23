#!/user/bin/env bash


APP=gmall


if [ -n "$2" ]; then
    do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

    
dim_user_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dim_user_info partition(dt='9999-99-99')
    select id,
           login_name,
           nick_name,
           md5(name),
           md5(phone_num),
           md5(email),
           user_level,
           birthday,
           gender,
           create_time,
           operate_time,
           '$do_date',
           '9999-99-99'
    from ${APP}.ods_user_info
    where dt='${do_date}';"
    
dim_sku_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    with sku as
    (
        select id,
               price,
               sku_name,
               sku_desc,
               weight,
               is_sale,
               spu_id,
               category3_id,
               tm_id,
               create_time
        from ${APP}.ods_sku_info
        where dt='${do_date}'
    ),
    spu as
    (
        select id,
               spu_name
        from ${APP}.ods_spu_info
        where dt='${do_date}'
    ),
    c3 as
    (
        select id,
               name,
               category2_id
        from ${APP}.ods_base_category3
        where dt='${do_date}'
    ),
    c2 as
    (
        select id,
               name,
               category1_id
        from ${APP}.ods_base_category2
        where dt='${do_date}'
    ),
    c1 as
    (
        select id,
               name
        from ${APP}.ods_base_category1
        where dt='${do_date}'
    ),
    tm as
    (
        select id,
               tm_name
        from ${APP}.ods_base_trademark
        where dt='${do_date}'
    ),
    attr as
    (
        select sku_id,
               collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
        from ${APP}.ods_sku_attr_value
        where dt='${do_date}'
        group by sku_id
    ),
    sale_attr as
    (
        select sku_id,
               collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
        from ${APP}.ods_sku_sale_attr_value
        where dt='${do_date}'
        group by sku_id
    )
    insert overwrite table ${APP}.dim_sku_info partition(dt='${do_date}')
    select sku.id,
           sku.price,
           sku.sku_name,
           sku.sku_desc,
           sku.weight,
           sku.is_sale,
           sku.spu_id,
           spu.spu_name,
           sku.category3_id,
           c3.name,
           c3.category2_id,
           c2.name,
           c2.category1_id,
           c1.name,
           sku.tm_id,
           tm.tm_name,
           attr.attrs,
           sale_attr.sale_attrs,
           sku.create_time
    from sku left join spu       on sku.spu_id       = spu.id
             left join c3        on sku.category3_id = c3.id
             left join c2        on c3.category2_id  = c2.id
             left join c1        on c2.category1_id  = c1.id
             left join tm        on sku.tm_id        = tm.id
             left join attr      on sku.id           = attr.sku_id
             left join sale_attr on sku.id           = sale_attr.sku_id;"
    
dim_base_province="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dim_base_province
    select bp.id,
           bp.name,
           bp.area_code,
           bp.iso_code,
           bp.iso_3166_2,
           bp.region_id,
           br.region_name
    from ${APP}.ods_base_province bp
    join ${APP}.ods_base_region br on bp.region_id = br.id;"

dim_coupon_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dim_coupon_info partition(dt='${do_date}')
    select id,
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
    from ${APP}.ods_coupon_info
    where dt='${do_date}';"
    
dim_activity_rule_info="
    set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
    insert overwrite table ${APP}.dim_activity_rule_info partition(dt='${do_date}')
    select ar.id,
        ar.activity_id,
        ai.activity_name,
        ar.activity_type,
        ai.start_time,
        ai.end_time,
        ai.create_time,
        ar.condition_amount,
        ar.condition_num,
        ar.benefit_amount,
        ar.benefit_discount,
        ar.benefit_level
    from
    (
        select id,
               activity_id,
               activity_type,
               condition_amount,
               condition_num,
               benefit_amount,
               benefit_discount,
               benefit_level
        from ${APP}.ods_activity_rule
        where dt='${do_date}'
    ) ar left join
    (
        select id,
               activity_name,
               start_time,
               end_time,
               create_time
        from ${APP}.ods_activity_info
        where dt='${do_date}'
    ) ai on ar.activity_id = ai.id; "
    
case $1 in
    "dim_user_info"){
        hive -e "${dim_user_info}"
    };;
    "dim_sku_info"){
        hive -e "${dim_sku_info}"
    };;
    "dim_base_province"){
        hive -e "${dim_base_province}"
    };;
    "dim_coupon_info"){
        hive -e "${dim_coupon_info}"
    };;
    "dim_activity_rule_info"){
        hive -e "${dim_activity_rule_info}"
    };;
    "all"){
        hive -e "${dim_user_info} ${dim_sku_info} ${dim_coupon_info} ${dim_activity_rule_info} ${dim_base_province}"
    };;
esac


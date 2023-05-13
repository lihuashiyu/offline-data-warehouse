-- -------------------------------------------------------------------------------------------------
-- 1. DIM 层的设计依据是维度建模理论，该层存储维度模型的维度表。
-- 2. DIM 层的数据存储格式为 orc 列式存储 + snappy 压缩。
-- 3. DIM 层表名的命名规范为 dim_表名_全量表或者拉链表标识（full/zip）
-- -------------------------------------------------------------------------------------------------


-- -------------------------------------------------------------------------------------------------
-- 商品维度表
-- -------------------------------------------------------------------------------------------------
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

-- 数据装载
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
    from ods_sku_info_full
    where dt = '2021-08-15'
),
spu  as (select id, spu_name           from ods_spu_info_full       where dt = '2021-08-15'),
c3   as (select id, name, category2_id from ods_base_category3_full where dt = '2021-08-15'),
c2   as (select id, name, category1_id from ods_base_category2_full where dt = '2021-08-15'),
c1   as (select id, name               from ods_base_category1_full where dt = '2021-08-15'),
tm   as (select id, tm_name            from ods_base_trademark_full where dt = '2021-08-15'),
attr as 
(
    select sku_id,
           collect_set
           (
               named_struct
               (
                   'attr_id',    attr_id, 
                   'value_id',   value_id, 
                   'attr_name',  attr_name, 
                   'value_name', value_name
               )
           )       as attrs
    from ods_sku_attr_value_full
    where dt = '2021-08-15'
    group by sku_id
),
sale_attr as 
(
    select sku_id, 
           collect_set
           (
               named_struct
               (
                   'sale_attr_id',         sale_attr_id,
                   'sale_attr_value_id',   sale_attr_value_id,
                   'sale_attr_name',       sale_attr_name,
                   'sale_attr_value_name', sale_attr_value_name
               )
           ) sale_attrs
    from ods_sku_sale_attr_value_full
    where dt = '2021-08-15'
    group by sku_id
)
insert overwrite table dim_sku_full
partition(dt = '2021-08-15')
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
         left join sale_attr on sku.id           = sale_attr.sku_id;


-- -------------------------------------------------------------------------------------------------
-- 优惠券维度表
-- -------------------------------------------------------------------------------------------------
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

-- 数据装载
insert overwrite table dim_coupon_full partition (dt = '2021-08-15')
select coupon_info.id,
       coupon_info.coupon_name,
       coupon_info.coupon_type          as coupon_type_code,
       coupon_dic.dic_name              as coupon_type_name,
       coupon_info.condition_amount,
       coupon_info.condition_num,
       coupon_info.activity_id,
       coupon_info.benefit_amount,
       coupon_info.benefit_discount,
       case coupon_type
           when '3201' then concat('满', coupon_info.condition_amount, '元减', coupon_info.benefit_amount,              '元')
           when '3202' then concat('满', coupon_info.condition_num,    '件打', 10 * (1 - coupon_info.benefit_discount), '折')
           when '3203' then concat('减', coupon_info.benefit_amount,   '元')
       end                              as benefit_rule,
       coupon_info.create_time,
       coupon_info.range_type           as range_type_code,
       range_dic.dic_name               as range_type_name,
       coupon_info.limit_num,
       coupon_info.taken_count,
       coupon_info.start_time,
       coupon_info.end_time,
       coupon_info.operate_time,
       coupon_info.expire_time
from 
(
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
    from ods_coupon_info_full
    where dt = '2021-08-15'
) as coupon_info left join 
( 
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '32'
) as coupon_dic 
    on coupon_info.coupon_type = coupon_dic.dic_code 
left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '33'
) as range_dic 
    on coupon_info.range_type = range_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 活动维度表
-- -------------------------------------------------------------------------------------------------
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

-- 数据装载
insert overwrite table dim_activity_full partition (dt = '2021-08-15')
select rule.id                 as activity_rule_id,
       info.id                 as activity_id,
       info.activity_name,
       rule.activity_type      as activity_type_code,
       dic.dic_name            as activity_type_name,
       info.activity_desc,
       info.start_time,
       info.end_time,
       info.create_time,
       rule.condition_amount,
       rule.condition_num,
       rule.benefit_amount,
       rule.benefit_discount,
       case rule.activity_type
           when '3101' then concat('满', rule.condition_amount,           '元减', rule.benefit_amount,              '元')
           when '3102' then concat('满', rule.condition_num,              '件打', 10 * (1 - rule.benefit_discount), '折')
           when '3103' then concat('打', 10 * (1 - rule.benefit_discount), '折')
       end                     as benefit_rule,
       rule.benefit_level
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
    from ods_activity_rule_full
    where dt = '2021-08-15'
) as rule left join 
(
    select id, 
           activity_name, 
           activity_type, 
           activity_desc, 
           start_time,
           end_time, 
           create_time
    from ods_activity_info_full
    where dt = '2021-08-15'
) as info on rule.activity_id = info.id left join 
(
    select dic_code, 
           dic_name
    from ods_base_dic_full
    where dt = '2021-08-15' and parent_code = '31'
) as dic on rule.activity_type = dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 地区维度表
-- -------------------------------------------------------------------------------------------------
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

-- 数据装载
insert overwrite table dim_province_full partition (dt = '2021-08-15')
select province.id,
       province.name        as province_name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       province.region_id,
       region.region_name
from 
(
    select id, 
           name, 
           region_id, 
           area_code, 
           iso_code, 
           iso_3166_2
   from ods_base_province_full
   where dt = '2021-08-15'
) as province left join 
(
    select id, 
           region_name 
    from ods_base_region_full 
    where dt = '2021-08-15'
) as region 
    on province.region_id = region.id;


-- -------------------------------------------------------------------------------------------------
-- 日期维度表
-- -------------------------------------------------------------------------------------------------
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

-- 数据装载：通常情况下，时间维度表的数据并不是来自于业务系统，而是手动写入，
--          并且由于时间维度表数据的可预见性，无须每日导入，一般可一次性导入一年的数据。
-- 创建临时表
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

select * from tmp_dim_date_info;

-- 将数据文件上传到 HFDS 上临时表路径：/warehouse/tmp/tmp_dim_date_info
-- 执行以下语句将其导入时间维度表
insert overwrite table dim_date (date_id, week_id, week_day, day, month, quarter, year, is_workday, holiday_id) 
select date_id, week_id, week_day, day, month, quarter, year, is_workday, holiday_id from tmp_dim_date_info;

-- 检查数据是否导入成功
select * from dim_date;


-- -------------------------------------------------------------------------------------------------
-- 用户维度表
-- -------------------------------------------------------------------------------------------------
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

-- 首日装载
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
select data.id,
       data.login_name,
       data.nick_name,
       md5(data.name)       as name,
       md5(data.phone_num)  as phone_num,
       md5(data.email)      as email,
       data.user_level,
       data.birthday,
       data.gender,
       data.create_time,
       data.operate_time,
       '2021-08-15'         as start_date,
       '9999-12-31'         as end_date
from ods_user_info_inc
where dt = '2021-08-15' and type = 'bootstrap-insert';

-- 每日装载
with tmp as 
(
    select old.id           as old_id,
           old.login_name   as old_login_name,
           old.nick_name    as old_nick_name,
           old.name         as old_name,
           old.phone_num    as old_phone_num,
           old.email        as old_email,
           old.user_level   as old_user_level,
           old.birthday     as old_birthday,
           old.gender       as old_gender,
           old.create_time  as old_create_time,
           old.operate_time as old_operate_time,
           old.start_date   as old_start_date,
           old.end_date     as old_end_date,
           new.id           as new_id,
           new.login_name   as new_login_name,
           new.nick_name    as new_nick_name,
           new.name         as new_name,
           new.phone_num    as new_phone_num,
           new.email        as new_email,
           new.user_level   as new_user_level,
           new.birthday     as new_birthday,
           new.gender       as new_gender,
           new.create_time  as new_create_time,
           new.operate_time as new_operate_time,
           new.start_date   as new_start_date,
           new.end_date     as new_end_date
    from 
    (
        select id,
               login_name,
               nick_name,
               name,
               phone_num,
               email,
               user_level,
               birthday,
               gender,
               create_time,
               operate_time,
               start_date,
               end_date
        from dim_user_zip
        where dt = '9999-12-31'
    ) as old full outer join 
    (
        select id,
               login_name,
               nick_name,
               md5(name)      as name,
               md5(phone_num) as phone_num,
               md5(email)     as email,
               user_level,
               birthday,
               gender,
               create_time,
               operate_time,
               '2021-08-15'   as start_date,
               '9999-12-31'   as end_date
	    from 
	    (
	        select data.id,
	               data.login_name,
	               data.nick_name,
	               data.name,
	               data.phone_num,
	               data.email,
	               data.user_level,
	               data.birthday,
	               data.gender,
	               data.create_time,
	               data.operate_time,
	               row_number() over (partition by data.id order by ts desc) as rn
		    from ods_user_info_inc
		    where dt = '2021-08-15'
	   ) as user_info where user_info.rn = 1
    ) as new on old.id = new.id
)
insert overwrite table dim_user_zip partition(dt)
select if(tmp.new_id is not null, tmp.new_id,           tmp.old_id)           as id,
       if(tmp.new_id is not null, tmp.new_login_name,   tmp.old_login_name)   as login_name,
       if(tmp.new_id is not null, tmp.new_nick_name,    tmp.old_nick_name)    as nick_name,
       if(tmp.new_id is not null, tmp.new_name,         tmp.old_name)         as name,
       if(tmp.new_id is not null, tmp.new_phone_num,    tmp.old_phone_num)    as phone_num,
       if(tmp.new_id is not null, tmp.new_email,        tmp.old_email)        as email,
       if(tmp.new_id is not null, tmp.new_user_level,   tmp.old_user_level)   as user_level,
       if(tmp.new_id is not null, tmp.new_birthday,     tmp.old_birthday)     as birthday,
       if(tmp.new_id is not null, tmp.new_gender,       tmp.old_gender)       as gender,
       if(tmp.new_id is not null, tmp.new_create_time,  tmp.old_create_time)  as create_time,
       if(tmp.new_id is not null, tmp.new_operate_time, tmp.old_operate_time) as operate_time,
       if(tmp.new_id is not null, tmp.new_start_date,   tmp.old_start_date)   as start_date,
       if(tmp.new_id is not null, tmp.new_end_date,     tmp.old_end_date)     as end_date,
       if(tmp.new_id is not null, tmp.new_end_date,     tmp.old_end_date)     as dt
from tmp 
union all
select tmp.old_id                                 as id,
       tmp.old_login_name                         as login_name,
       tmp.old_nick_name                          as nick_name,
       tmp.old_name                               as name,
       tmp.old_phone_num                          as phone_num,
       tmp.old_email                              as email,
       tmp.old_user_level                         as user_level,
       tmp.old_birthday                           as birthday,
       tmp.old_gender                             as gender,
       tmp.old_create_time                        as create_time,
       tmp.old_operate_time                       as operate_time,
       tmp.old_start_date                         as start_date,
       cast(date_add('2021-08-15', -1) as string) as end_date,
       cast(date_add('2021-08-15', -1) as string) as dt
from tmp
where tmp.old_id is not null and tmp.new_id is not null;


-- -------------------------------------------------------------------------------------------------
-- DIM 层首日装载脚本：ods-dim-init.sh all 2021-08-15
-- DIM 层每日装载脚本：ods-dim.sh      all 2021-08-15
-- -------------------------------------------------------------------------------------------------
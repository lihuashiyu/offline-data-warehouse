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
    id                   string                                                              comment 'sku_id',
    price                decimal(16, 2)                                                      comment '商品价格',
    sku_name             string                                                              comment '商品名称',
    sku_desc             string                                                              comment '商品描述',
    weight               decimal(16, 2)                                                      comment '重量',
    is_sale              boolean                                                             comment '是否在售',
    spu_id               string                                                              comment 'spu编号',
    spu_name             string                                                              comment 'spu名称',
    category3_id         string                                                              comment '三级分类id',
    category3_name       string                                                              comment '三级分类名称',
    category2_id         string                                                              comment '二级分类id',
    category2_name       string                                                              comment '二级分类名称',
    category1_id         string                                                              comment '一级分类id',
    category1_name       string                                                              comment '一级分类名称',
    tm_id                string                                                              comment '品牌id',
    tm_name              string                                                              comment '品牌名称',
    sku_attr_values      array<struct<attr_id: string,        value_id: string,           
                                      attr_name: string,      value_name: string>>           comment '平台属性',
    sku_sale_attr_values array<struct<sale_attr_id: string,   sale_attr_value_id: string, 
                                      sale_attr_name: string, sale_attr_value_name: string>> comment '销售属性',
    create_time          string comment '创建时间'
) comment '商品维度表' partitioned by (dt string) 
    stored as orc location '/warehouse/dim/dim_sku_full/' 
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
spu as (select id, spu_name from ods_spu_info_full where dt = '2021-08-15'),
c3 as (select id, name, category2_id from ods_base_category3_full where dt = '2021-08-15'),
c2 as (select id, name, category1_id from ods_base_category2_full where dt = '2021-08-15'),
c1 as (select id, name from ods_base_category1_full where dt = '2021-08-15'),
tm as (select id, tm_name from ods_base_trademark_full where dt = '2021-08-15'),
attr as 
(
    select sku_id,
           collect_set
           (
               named_struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name, 'value_name', value_name)
           ) attrs
    from ods_sku_attr_value_full
    where dt = '2021-08-15'
    group by sku_id
),
sale_attr as 
(
    select sku_id, 
           collect_set
           (
               named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id, 'sale_attr_name', 
                   sale_attr_name, 'sale_attr_value_name', sale_attr_value_name)
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
from sku left join spu       on sku.spu_id = spu.id
         left join c3        on sku.category3_id = c3.id
         left join c2        on c3.category2_id = c2.id
         left join c1        on c2.category1_id = c1.id
         left join tm        on sku.tm_id = tm.id
         left join attr      on sku.id = attr.sku_id
         left join sale_attr on sku.id = sale_attr.sku_id;


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
    benefit_rule     string         comment '优惠规则:满元*减*元，满*件打*折',
    create_time      string         comment '创建时间',
    range_type_code  string         comment '优惠范围类型编码',
    range_type_name  string         comment '优惠范围类型名称',
    limit_num        bigint         comment '最多领取次数',
    taken_count      bigint         comment '已领取次数',
    start_time       string         comment '可以领取的开始日期',
    end_time         string         comment '可以领取的结束日期',
    operate_time     string         comment '修改时间',
    expire_time      string         comment '过期时间'
) comment '优惠券维度表' partitioned by (dt string) 
    stored as orc location '/warehouse/dim/dim_coupon_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_coupon_full partition (dt = '2021-08-15')
    select id,
           coupon_name,
           coupon_type,
           coupon_dic.dic_name,
           condition_amount,
           condition_num,
           activity_id,
           benefit_amount,
           benefit_discount,
           case coupon_type
               when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
               when '3202' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
               when '3203' then concat('减', benefit_amount, '元')
           end benefit_rule,
           create_time,
           range_type,
           range_dic.dic_name,
           limit_num,
           taken_count,
           start_time,
           end_time,
           operate_time,
           expire_time
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
    ) ci left join 
    ( 
        select dic_code, 
               dic_name
        from ods_base_dic_full
        where dt = '2021-08-15' and parent_code = '32'
    ) coupon_dic on ci.coupon_type = coupon_dic.dic_code left join 
    (
        select dic_code, 
               dic_name
        from ods_base_dic_full
        where dt = '2021-08-15' and parent_code = '33'
    ) range_dic on ci.range_type = range_dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 活动维度表
-- -------------------------------------------------------------------------------------------------
drop table if exists dim_activity_full;
create external table  if not exists dim_activity_full
(
    activity_rule_id   string         comment '活动规则ID',
    activity_id        string         comment '活动ID',
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
) comment '活动信息表' partitioned by (dt string) 
    stored as orc location '/warehouse/dim/dim_activity_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_activity_full partition (dt = '2021-08-15')
    select rule.id,
           info.id,
           activity_name,
           rule.activity_type,
           dic.dic_name,
           activity_desc,
           start_time,
           end_time,
           create_time,
           condition_amount,
           condition_num,
           benefit_amount,
           benefit_discount,
           case rule.activity_type
               when '3101' then concat('满', condition_amount, '元减', benefit_amount, '元')
               when '3102' then concat('满', condition_num, '件打', 10 * (1 - benefit_discount), '折')
               when '3103' then concat('打', 10 * (1 - benefit_discount), '折')
           end benefit_rule,
           benefit_level
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
    ) rule left join 
    (
        select id, activity_name, 
               activity_type, 
               activity_desc, 
               start_time,
               end_time, 
               create_time
        from ods_activity_info_full
        where dt = '2021-08-15'
    ) info on rule.activity_id = info.id left join 
    (
        select dic_code, 
               dic_name
        from ods_base_dic_full
        where dt = '2021-08-15' and parent_code = '31'
    ) dic on rule.activity_type = dic.dic_code;


-- -------------------------------------------------------------------------------------------------
-- 地区维度表
-- -------------------------------------------------------------------------------------------------
drop table if exists dim_province_full;
create external table  if not exists dim_province_full
(
    id            string comment 'id',
    province_name string comment '省市名称',
    area_code     string comment '地区编码',
    iso_code      string comment '旧版 ISO-3166-2 编码，供可视化使用',
    iso_3166_2    string comment '新版 IOS-3166-2 编码，供可视化使用',
    region_id     string comment '地区 ID',
    region_name   string comment '地区名称'
) comment '地区维度表' partitioned by (dt string) 
    stored as orc location '/warehouse/dim/dim_province_full/' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载
insert overwrite table dim_province_full partition (dt = '2021-08-15')
    select province.id,
           province.name,
           province.area_code,
           province.iso_code,
           province.iso_3166_2,
           region_id,
           region_name
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
    ) province left join 
    (
        select id, 
               region_name 
        from ods_base_region_full 
        where dt = '2021-08-15'
    ) region on province.region_id = region.id;


-- -------------------------------------------------------------------------------------------------
-- 日期维度表
-- -------------------------------------------------------------------------------------------------
drop table if exists dim_date;
create external table  if not exists dim_date
(
    date_id    string comment '日期ID',
    week_id    string comment '周ID,一年中的第几周',
    week_day   string comment '周几',
    day        string comment '每月的第几天',
    month      string comment '一年中的第几月',
    quarter    string comment '一年中的第几季度',
    year       string comment '年份',
    is_workday string comment '是否是工作日',
    holiday_id string comment '节假日'
) comment '时间维度表' stored as orc location '/warehouse/dim/dim_date/' 
    tblproperties ('orc.compress' = 'snappy');

-- 数据装载：通常情况下，时间维度表的数据并不是来自于业务系统，而是手动写入，
--          并且由于时间维度表数据的可预见性，无须每日导入，一般可一次性导入一年的数据。
-- 创建临时表
drop table if exists tmp_dim_date_info;
create external table  if not exists tmp_dim_date_info
(
    date_id    string comment '日',
    week_id    string comment '周ID',
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

-- 将数据文件上传到 HFDS 上临时表路径/warehouse/gmall/tmp/tmp_dim_date_info
-- 执行以下语句将其导入时间维度表
insert overwrite table dim_date select * from tmp_dim_date_info;

-- 检查数据是否导入成功
select * from dim_date;


-- -------------------------------------------------------------------------------------------------
-- 用户维度表
-- -------------------------------------------------------------------------------------------------
drop table if exists dim_user_zip;
create external table  if not exists dim_user_zip
(
    id           string comment '用户id',
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
) comment '用户表' partitioned by (dt string) stored as orc location '/warehouse/dim/dim_user_zip/' 
    tblproperties ('orc.compress' = 'snappy');

-- 首日装载
insert overwrite table dim_user_zip partition (dt = '9999-12-31')
    select data.id,
           data.login_name,
           data.nick_name,
           md5(data.name),
           md5(data.phone_num),
           md5(data.email),
           data.user_level,
           data.birthday,
           data.gender,
           data.create_time,
           data.operate_time,
           '2021-08-15' start_date,
           '9999-12-31' end_date
	from ods_user_info_inc
	where dt = '2021-08-15' and type = 'insert';
-- 	where dt = '2021-08-15' and type = 'bootstrap-insert';

select * from dim_user_zip;

-- 每日装载
with tmp as 
(
    select old.id           old_id,
           old.login_name   old_login_name,
           old.nick_name    old_nick_name,
           old.name         old_name,
           old.phone_num    old_phone_num,
           old.email        old_email,
           old.user_level   old_user_level,
           old.birthday     old_birthday,
           old.gender       old_gender,
           old.create_time  old_create_time,
           old.operate_time old_operate_time,
           old.start_date   old_start_date,
           old.end_date     old_end_date,
           new.id           new_id,
           new.login_name   new_login_name,
           new.nick_name    new_nick_name,
           new.name         new_name,
           new.phone_num    new_phone_num,
           new.email        new_email,
           new.user_level   new_user_level,
           new.birthday     new_birthday,
           new.gender       new_gender,
           new.create_time  new_create_time,
           new.operate_time new_operate_time,
           new.start_date   new_start_date,
           new.end_date     new_end_date
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
    ) old full outer join 
    (
        select id,
               login_name,
               nick_name,
               md5(name)      name,
               md5(phone_num) phone_num,
               md5(email)     email,
               user_level,
               birthday,
               gender,
               create_time,
               operate_time,
               '2020-06-15'   start_date,
               '9999-12-31'   end_date
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
	               row_number() over (partition by data.id order by ts desc) rn
		    from ods_user_info_inc
		    where dt = '2020-06-15'
	   ) t1 where rn = 1
    ) new on old.id = new.id
)
insert overwrite table dim_user_zip partition(dt)
	select if(new_id is not null, new_id,           old_id),
	       if(new_id is not null, new_login_name,   old_login_name),
	       if(new_id is not null, new_nick_name,    old_nick_name),
	       if(new_id is not null, new_name,         old_name),
	       if(new_id is not null, new_phone_num,    old_phone_num),
	       if(new_id is not null, new_email,        old_email),
	       if(new_id is not null, new_user_level,   old_user_level),
	       if(new_id is not null, new_birthday,     old_birthday),
	       if(new_id is not null, new_gender,       old_gender),
	       if(new_id is not null, new_create_time,  old_create_time),
	       if(new_id is not null, new_operate_time, old_operate_time),
	       if(new_id is not null, new_start_date,   old_start_date),
	       if(new_id is not null, new_end_date,     old_end_date),
	       if(new_id is not null, new_end_date,     old_end_date) dt
	from tmp union all
	select old_id,
	       old_login_name,
	       old_nick_name,
	       old_name,
	       old_phone_num,
	       old_email,
	       old_user_level,
	       old_birthday,
	       old_gender,
	       old_create_time,
	       old_operate_time,
	       old_start_date,
	       cast(date_add('2020-06-15', -1) as string) old_end_date,
	       cast(date_add('2020-06-15', -1) as string) dt
	from tmp
	where old_id is not null and new_id is not null;

-- 首日装载脚本    ：ods_to_dim_init.sh
-- 增加脚本执行权限：ods_to_dim_init.sh all 2021-08-15


-- DIM 层首日装载脚本：ods_dim_init.sh all 2021-08-15
-- DIM 层每日装载脚本：ods_dim.sh all 2021-08-15 

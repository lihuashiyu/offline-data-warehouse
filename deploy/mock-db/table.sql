# drop database if exists at_gui_gu;
# create database if not exists at_gui_gu;
# use at_gui_gu;

-- -------------------------------------------------------------------------------------------------
-- Table structure for activity_info
-- -------------------------------------------------------------------------------------------------
drop table if exists activity_info;
create table if not exists activity_info
(
    id            bigint(20) primary key auto_increment comment '活动id',
    activity_name varchar(200)                          comment '活动名称',
    activity_type varchar(10)                           comment '活动类型（1：满减，2：折扣）',
    activity_desc varchar(2000)                         comment '活动描述',
    start_time    datetime(0)                           comment '开始时间',
    end_time      datetime(0)                           comment '结束时间',
    create_time   datetime(0)                           comment '创建时间'
) engine = InnoDB auto_increment = 1  comment = '活动表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for activity_rule
-- -------------------------------------------------------------------------------------------------
drop table if exists activity_rule;
create table if not exists activity_rule
(
    id               int(11) primary key auto_increment comment '编号',
    activity_id      int(11)                            comment '类型',
    activity_type    varchar(20)                        comment '活动类型',
    condition_amount decimal(16, 2)                     comment '满减金额',
    condition_num    bigint(20)                         comment '满减件数',
    benefit_amount   decimal(16, 2)                     comment '优惠金额',
    benefit_discount decimal(10, 2)                     comment '优惠折扣',
    benefit_level    bigint(20)                         comment '优惠级别'
) engine = InnoDB auto_increment = 1 comment = '优惠规则';

-- -------------------------------------------------------------------------------------------------
-- Table structure for activity_sku
-- -------------------------------------------------------------------------------------------------
drop table if exists activity_sku;
create table if not exists activity_sku
(
    id          bigint(20) primary key auto_increment comment '编号',
    activity_id bigint(20)                            comment '活动 id ',
    sku_id      bigint(20)                            comment 'sku_id',
    create_time datetime(0)                           comment '创建时间'
) engine = InnoDB auto_increment = 1  comment = '活动参与商品';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_attr_info
-- -------------------------------------------------------------------------------------------------
drop table if exists base_attr_info;
create table if not exists base_attr_info
(
    id             bigint(20)   primary key auto_increment comment '编号',
    attr_name      varchar(100) not null                   comment '属性名称',
    category_id    bigint(20)                              comment '分类id',
    category_level int(11)                                 comment '分类层级'
) engine = InnoDB auto_increment = 1  comment = '属性表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_attr_value
-- -------------------------------------------------------------------------------------------------
drop table if exists base_attr_value;
create table if not exists base_attr_value
(
    id         bigint(20)   primary key auto_increment comment '编号',
    value_name varchar(100) not null                   comment '属性值名称',
    attr_id    bigint(20)                              comment '属性id'
) engine = InnoDB auto_increment = 1  comment = '属性值表'
 ;

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_category1
-- -------------------------------------------------------------------------------------------------
drop table if exists base_category1;
create table if not exists base_category1
(
    id   bigint(20)  primary key auto_increment comment '编号',
    name varchar(10) not null                   comment '分类名称'
) engine = InnoDB auto_increment = 1  comment = '一级分类表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_category2
-- -------------------------------------------------------------------------------------------------
drop table if exists base_category2;
create table if not exists base_category2
(
    id           bigint(20)   primary key auto_increment comment '编号',
    name         varchar(200) not null                   comment '二级分类名称',
    category1_id bigint(20)                              comment '一级分类编号'
) engine = InnoDB auto_increment = 1  comment = '二级分类表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_category3
-- -------------------------------------------------------------------------------------------------
drop table if exists base_category3;
create table if not exists base_category3
(
    id           bigint(20)   primary key auto_increment comment '编号',
    name         varchar(200) not null                   comment '三级分类名称',
    category2_id bigint(20)                              comment '二级分类编号'
) engine = InnoDB auto_increment = 1 comment = '三级分类表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_dic
-- -------------------------------------------------------------------------------------------------
drop table if exists base_dic;
create table if not exists base_dic
(
    id           int(7)   primary key auto_increment comment '编号',
    dic_code     varchar(10) not null                comment '编号',
    dic_name     varchar(100)                        comment '编码名称',
    parent_code  varchar(10)                         comment '父编号',
    create_time  datetime(0)                         comment '创建日期',
    operate_time datetime(0)                         comment '修改日期'
) engine = InnoDB auto_increment = 1;

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_frontend_param
-- -------------------------------------------------------------------------------------------------
drop table if exists base_frontend_param;
create table if not exists base_frontend_param
(
    id        bigint(20)   primary key auto_increment comment '编号',
    code      varchar(100) not null                   comment '属性名称',
    delete_id bigint(20)                              comment '分类id'
) engine = InnoDB auto_increment = 1  comment = '前端数据保护表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_province
-- -------------------------------------------------------------------------------------------------
drop table if exists base_province;
create table if not exists base_province
(
    id         int(7)      primary key comment 'id',
    name       varchar(20)             comment '省名称',
    region_id  varchar(20)             comment '大区id',
    area_code  varchar(20)             comment '行政区位码',
    iso_code   varchar(20)             comment '国际编码',
    iso_3166_2 varchar(20)             comment 'ISO3166编码'
) engine = InnoDB ;

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_region
-- -------------------------------------------------------------------------------------------------
drop table if exists base_region;
create table if not exists base_region
(
    id varchar(20) primary key comment '大区id', 
    region_name varchar(20)    comment '大区名称'
) engine = InnoDB;

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_sale_attr
-- -------------------------------------------------------------------------------------------------
drop table if exists base_sale_attr;
create table if not exists base_sale_attr
(
    id   bigint(20)  primary key auto_increment comment '编号',
    name varchar(20) not null                   comment '销售属性名称'
) engine = InnoDB auto_increment = 1  comment = '基本销售属性表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for base_trademark
-- -------------------------------------------------------------------------------------------------
drop table if exists base_trademark;
create table if not exists base_trademark
(
    id       bigint(20)   primary key auto_increment comment '编号',
    tm_name  varchar(100) not null                   comment '属性值',
    logo_url varchar(200)                            comment '品牌 logo 的图片路径'
) engine = InnoDB auto_increment = 1  comment = '品牌表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for cart_info
-- -------------------------------------------------------------------------------------------------
drop table if exists cart_info;
create table if not exists cart_info
(
    id           bigint(20) primary key auto_increment comment '编号',
    user_id      varchar(200)                          comment '用户id',
    sku_id       bigint(20)                            comment 'skuid',
    cart_price   decimal(10, 2)                        comment '放入购物车时价格',
    sku_num      int(11)                               comment '数量',
    img_url      varchar(200)                          comment '图片文件',
    sku_name     varchar(200)                          comment 'sku名称 (冗余)',
    is_checked   int(1)                                comment '是否检查',
    create_time  datetime(0)                           comment '创建时间',
    operate_time datetime(0)                           comment '修改时间',
    is_ordered   bigint(20)                            comment '是否已经下单',
    order_time   datetime(0)                           comment '下单时间',
    source_type  varchar(20)                           comment '来源类型',
    source_id    bigint(20)                            comment '来源编号'
) engine = InnoDB auto_increment = 1 comment = '购物车表 用户登录系统时更新冗余';

-- -------------------------------------------------------------------------------------------------
-- Table structure for cms_banner
-- -------------------------------------------------------------------------------------------------
drop table if exists cms_banner;
create table if not exists cms_banner
(
    id        bigint(20)   primary key auto_increment   comment 'ID',
    title     varchar(20)                    default '' comment '标题',
    image_url varchar(500)          not null default '' comment '图片地址',
    link_url  varchar(500)                   default '' comment '链接地址',
    sort      int(10)      unsigned not null default 0  comment '排序'
) engine = InnoDB auto_increment = 1 comment = '首页banner表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for comment_info
-- -------------------------------------------------------------------------------------------------
drop table if exists comment_info;
create table if not exists comment_info
(
    id           bigint(20)   primary key auto_increment comment '编号',
    user_id      bigint(20)                              comment '用户id',
    nick_name    varchar(20)                             comment '用户昵称',
    head_img     varchar(200) ,
    sku_id       bigint(20)                              comment 'skuid',
    spu_id       bigint(20)                              comment '商品id',
    order_id     bigint(20)                              comment '订单编号',
    appraise     varchar(10)                             comment '评价 1 好评 2 中评 3 差评',
    comment_txt  varchar(2000)                           comment '评价内容',
    create_time  datetime(0)                             comment '创建时间',
    operate_time datetime(0)                             comment '修改时间'
) engine = InnoDB auto_increment = 1 comment = '商品评论表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for coupon_info
-- -------------------------------------------------------------------------------------------------
drop table if exists coupon_info;
create table if not exists coupon_info
(
    id               bigint(20) primary key auto_increment comment '购物券编号',
    coupon_name      varchar(100)                          comment '购物券名称',
    coupon_type      varchar(10)                           comment '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    condition_amount decimal(10, 2)                        comment '满额数（3）',
    condition_num    bigint(20)                            comment '满件数（4）',
    activity_id      bigint(20)                            comment '活动编号',
    benefit_amount   decimal(16, 2)                        comment '减金额（1 3）',
    benefit_discount decimal(10, 2)                        comment '折扣（2 4）',
    create_time      datetime(0)                           comment '创建时间',
    range_type       varchar(10)                           comment '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    limit_num        int(11)        not null default 0     comment '最多领用次数',
    taken_count      int(11)        not null default 0     comment '已领用次数',
    start_time       datetime(0)                           comment '可以领取的开始日期',
    end_time         datetime(0)                           comment '可以领取的结束日期',
    operate_time     datetime(0)                           comment '修改时间',
    expire_time      datetime(0)                           comment '过期时间',
    range_desc       varchar(500)                          comment '范围描述'
) engine = InnoDB auto_increment = 1 comment = '优惠券表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for coupon_range
-- -------------------------------------------------------------------------------------------------
drop table if exists coupon_range;
create table if not exists coupon_range
(
    id         bigint(20)  primary key auto_increment comment '购物券编号',
    coupon_id  bigint(20)  not null default 0         comment '优惠券id',
    range_type varchar(10) not null default ''        comment '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    range_id   bigint(20)  not null default 0
) engine = InnoDB auto_increment = 1  comment = '优惠券范围表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for coupon_use
-- -------------------------------------------------------------------------------------------------
drop table if exists coupon_use;
create table if not exists coupon_use
(
    id            bigint(20) primary key auto_increment comment '编号',
    coupon_id     bigint(20)                            comment '购物券 ID',
    user_id       bigint(20)                            comment '用户 ID',
    order_id      bigint(20)                            comment '订单 ID',
    coupon_status varchar(10)                           comment '购物券状态（1：未使用 2：已使用）',
    get_time      datetime(0)                           comment '获取时间',
    using_time    datetime(0)                           comment '使用时间',
    used_time     datetime(0)                           comment '支付时间',
    expire_time   datetime(0)                           comment '过期时间'
) engine = InnoDB auto_increment = 1  comment = '优惠券领用表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for favor_info
-- -------------------------------------------------------------------------------------------------
drop table if exists favor_info;
create table if not exists favor_info
(
    id          bigint(20) primary key auto_increment comment '编号',
    user_id     bigint(20)                            comment '用户名称',
    sku_id      bigint(20)                            comment 'skuid',
    spu_id      bigint(20)                            comment '商品id',
    is_cancel   varchar(1)                            comment '是否已取消 0 正常 1 已取消',
    create_time datetime(0)                           comment '创建时间',
    cancel_time datetime(0)                           comment '修改时间'
) engine = InnoDB auto_increment = 1  comment = '商品收藏表';


-- -------------------------------------------------------------------------------------------------
-- Table structure for financial_sku_cost
-- -------------------------------------------------------------------------------------------------
drop table if exists financial_sku_cost;
create table if not exists financial_sku_cost
(
    id          varchar(20) primary key comment '主键',
    sku_id      bigint(20)              comment 'sku_id',
    sku_name    varchar(20)             comment '商品名称',
    busi_date   varchar(20)             comment '业务日期',
    is_lastest  varchar(2)              comment '是否最近',
    sku_cost    decimal(16, 2)          comment '商品结算成本',
    create_time datetime                comment '创建时间'
) engine = InnoDB ;

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_detail
-- -------------------------------------------------------------------------------------------------
drop table if exists order_detail;
create table if not exists order_detail
(
    id                    bigint(20) primary key auto_increment comment '编号',
    order_id              bigint(20)                            comment '订单编号',
    sku_id                bigint(20)                            comment 'sku_id',
    sku_name              varchar(200)                          comment 'sku名称（冗余)',
    img_url               varchar(200)                          comment '图片名称（冗余)',
    order_price           decimal(10, 2)                        comment '购买价格(下单时sku价格）',
    sku_num               bigint(20)                            comment '购买个数',
    create_time           datetime(0)                           comment '创建时间',
    source_type           varchar(20)                           comment '来源类型',
    source_id             bigint(20)                            comment '来源编号',
    split_total_amount    decimal(16, 2),
    split_activity_amount decimal(16, 2),
    split_coupon_amount   decimal(16, 2)
) engine = InnoDB auto_increment = 1 comment = '订单明细表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_detail_activity
-- -------------------------------------------------------------------------------------------------
drop table if exists order_detail_activity;
create table if not exists order_detail_activity
(
    id               bigint(20) primary key auto_increment comment '编号',
    order_id         bigint(20)                            comment '订单id',
    order_detail_id  bigint(20)                            comment '订单明细id',
    activity_id      bigint(20)                            comment '活动ID',
    activity_rule_id bigint(20)                            comment '活动规则',
    sku_id           bigint(20)                            comment 'skuID',
    create_time      datetime(0)                           comment '获取时间'
) engine = InnoDB auto_increment = 1 comment = '订单明细活动表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_detail_coupon
-- -------------------------------------------------------------------------------------------------
drop table if exists order_detail_coupon;
create table if not exists order_detail_coupon
(
    id              bigint(20) primary key auto_increment comment '编号',
    order_id        bigint(20)                            comment '订单id',
    order_detail_id bigint(20)                            comment '订单明细id',
    coupon_id       bigint(20)                            comment '购物券ID',
    coupon_use_id   bigint(20)                            comment '购物券领用id',
    sku_id          bigint(20)                            comment 'skuID',
    create_time     datetime(0)                           comment '获取时间'
) engine = InnoDB auto_increment = 1 comment = '订单明细购物券表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_info
-- -------------------------------------------------------------------------------------------------
drop table if exists order_info;
create table if not exists order_info
(
    id                     bigint(20) primary key auto_increment comment '编号',
    consignee              varchar(100)                          comment '收货人',
    consignee_tel          varchar(20)                           comment '收件人电话',
    total_amount           decimal(10, 2)                        comment '总金额',
    order_status           varchar(20)                           comment '订单状态',
    user_id                bigint(20)                            comment '用户id',
    payment_way            varchar(20)                           comment '付款方式',
    delivery_address       varchar(1000)                         comment '送货地址',
    order_comment          varchar(200)                          comment '订单备注',
    out_trade_no           varchar(50)                           comment '订单交易编号（第三方支付用)',
    trade_body             varchar(200)                          comment '订单描述(第三方支付用)',
    create_time            datetime(0)                           comment '创建时间',
    operate_time           datetime(0)                           comment '操作时间',
    expire_time            datetime(0)                           comment '失效时间',
    process_status         varchar(20)                           comment '进度状态',
    tracking_no            varchar(100)                          comment '物流单编号',
    parent_order_id        bigint(20)                            comment '父订单编号',
    img_url                varchar(200)                          comment '图片路径',
    province_id            int(20)                               comment '地区',
    activity_reduce_amount decimal(16, 2)                        comment '促销金额',
    coupon_reduce_amount   decimal(16, 2)                        comment '优惠券',
    original_total_amount  decimal(16, 2)                        comment '原价金额',
    feight_fee             decimal(16, 2)                        comment '运费',
    feight_fee_reduce      decimal(16, 2)                        comment '运费减免',
    refundable_time        datetime(0)                           comment '可退款日期（签收后30天）'
) engine = InnoDB auto_increment = 1  comment = '订单表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_refund_info
-- -------------------------------------------------------------------------------------------------
drop table if exists order_refund_info;
create table if not exists order_refund_info
(
    id                 bigint(20) primary key auto_increment comment '编号',
    user_id            bigint(20)                            comment '用户id',
    order_id           bigint(20)                            comment '订单id',
    sku_id             bigint(20)                            comment 'skuid',
    refund_type        varchar(20)                           comment '退款类型',
    refund_num         bigint(20)                            comment '退货件数',
    refund_amount      decimal(16, 2)                        comment '退款金额',
    refund_reason_type varchar(200)                          comment '原因类型',
    refund_reason_txt  varchar(20)                           comment '原因内容',
    refund_status      varchar(10)                           comment '退款状态（0：待审批 1：已退款）',
    create_time        datetime(0)                           comment '创建时间'
) engine = InnoDB auto_increment = 748  comment = '退单表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for order_status_log
-- -------------------------------------------------------------------------------------------------
drop table if exists order_status_log;
create table if not exists order_status_log
(
    id           bigint(11)   primary key auto_increment,
    order_id     bigint(11),
    order_status varchar(11),
    operate_time datetime(0)
) engine = InnoDB auto_increment = 1 comment = '订单状态日志记录表'; 

-- -------------------------------------------------------------------------------------------------
-- Table structure for payment_info
-- -------------------------------------------------------------------------------------------------
drop table if exists payment_info;
create table if not exists payment_info
(
    id               int(11)        primary key auto_increment comment '编号',
    out_trade_no     varchar(50)                        comment '对外业务编号',
    order_id         bigint(50)                         comment '订单编号',
    user_id          bigint(20)                         comment '用户 ID',
    payment_type     varchar(20)                        comment '支付类型（微信 支付宝）',
    trade_no         varchar(50)                        comment '交易编号',
    total_amount     decimal(10, 2)                     comment '支付金额',
    subject          varchar(200)                       comment '交易内容',
    payment_status   varchar(20)                        comment '支付状态',
    create_time      datetime(0)                        comment '创建时间',
    callback_time    datetime(0)                        comment '回调时间',
    callback_content text                               comment '回调信息'
) engine = InnoDB auto_increment = 1 comment = '支付信息表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for refund_payment
-- -------------------------------------------------------------------------------------------------
drop table if exists refund_payment;
create table if not exists refund_payment
(
    id               int(11)       primary key auto_increment comment '编号',
    out_trade_no     varchar(50)                              comment '对外业务编号',
    order_id         bigint(20)                               comment '订单编号',
    sku_id           bigint(20),
    payment_type     varchar(20)                              comment '支付类型（微信 支付宝）',
    trade_no         varchar(50)                              comment '交易编号',
    total_amount     decimal(10, 2)                           comment '退款金额',
    subject          varchar(200)                             comment '交易内容',
    refund_status    varchar(30)                              comment '退款状态',
    create_time      datetime(0)                              comment '创建时间',
    callback_time    datetime(0)                              comment '回调时间',
    callback_content text                                     comment '回调信息',
    index idx_out_trade_no (out_trade_no) using btree,
    index idx_order_id (order_id)         using btree
) engine = InnoDB auto_increment = 1 comment = '退款信息表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for seckill_goods
-- -------------------------------------------------------------------------------------------------
drop table if exists seckill_goods;
create table if not exists seckill_goods
(
    id              bigint(20)      primary key auto_increment,
    spu_id          bigint(20)                               comment 'spu_id',
    sku_id          bigint(20)                               comment 'sku_id',
    sku_name        varchar(100)                             comment '标题',
    sku_default_img varchar(150)                             comment '商品图片',
    price           decimal(10, 2)                           comment '原价格',
    cost_price      decimal(10, 2)                           comment '秒杀价格',
    create_time     datetime(0)                              comment '添加日期',
    check_time      datetime(0)                              comment '审核日期',
    status          varchar(1)                               comment '审核状态',
    start_time      datetime(0)                              comment '开始时间',
    end_time        datetime(0)                              comment '结束时间',
    num             int(11)                                  comment '秒杀商品数',
    stock_count     int(11)                                  comment '剩余库存数',
    sku_desc        varchar(2000)                            comment '描述'
) engine = InnoDB auto_increment = 1 comment = '商品秒杀表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for sku_attr_value
-- -------------------------------------------------------------------------------------------------
drop table if exists sku_attr_value;
create table if not exists sku_attr_value
(
    id         bigint(20) primary key auto_increment comment '编号',
    attr_id    bigint(20)                            comment '属性id（冗余)',
    value_id   bigint(20)                            comment '属性值id',
    sku_id     bigint(20)                            comment 'skuid',
    attr_name  varchar(30)                           comment '属性名',
    value_name varchar(30)                           comment '属性值名称'
) engine = InnoDB auto_increment = 1  comment = 'sku 平台属性值关联表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for sku_image
-- -------------------------------------------------------------------------------------------------
drop table if exists sku_image;
create table if not exists sku_image
(
    id         bigint(20)    primary key auto_increment comment '编号',
    sku_id     bigint(20)                               comment '商品id',
    img_name   varchar(200)                             comment '图片名称（冗余）',
    img_url    varchar(300)                             comment '图片路径(冗余)',
    spu_img_id bigint(20)                               comment '商品图片id',
    is_default varchar(4000)                            comment '是否默认'
) engine = InnoDB auto_increment = 1  comment = '库存单元图片表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for sku_info
-- -------------------------------------------------------------------------------------------------
drop table if exists sku_info;
create table if not exists sku_info
(
    id              bigint(20)     primary key auto_increment comment '库存id(itemID)',
    spu_id          bigint(20)                                comment '商品id',
    price           decimal(10, 0)                            comment '价格',
    sku_name        varchar(200)                              comment 'sku名称',
    sku_desc        varchar(2000)                             comment '商品规格描述',
    weight          decimal(10, 2)                            comment '重量',
    tm_id           bigint(20)                                comment '品牌(冗余)',
    category3_id    bigint(20)                                comment '三级分类id（冗余)',
    sku_default_img varchar(300)                              comment '默认显示图片(冗余)',
    is_sale         tinyint(3)     not null default 0         comment '是否销售（1：是 0：否）',
    create_time     datetime(0)                               comment '创建时间'
) engine = InnoDB auto_increment = 1  comment = '库存单元表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for sku_sale_attr_value
-- -------------------------------------------------------------------------------------------------
drop table if exists sku_sale_attr_value;
create table if not exists sku_sale_attr_value
(
    id                   bigint(20) primary key auto_increment comment 'id',
    sku_id               bigint(20)                            comment '库存单元id',
    spu_id               int(11)                               comment 'spu_id(冗余)',
    sale_attr_value_id   bigint(20)                            comment '销售属性值id',
    sale_attr_id         bigint(20),
    sale_attr_name       varchar(30),
    sale_attr_value_name varchar(30)
) engine = InnoDB auto_increment = 1 comment = 'sku销售属性值';

-- -------------------------------------------------------------------------------------------------
-- Table structure for spu_image
-- -------------------------------------------------------------------------------------------------
drop table if exists spu_image;
create table if not exists spu_image
(
    id       bigint(20)   primary key auto_increment comment '编号',
    spu_id   bigint(20)                              comment '商品id',
    img_name varchar(200)                            comment '图片名称',
    img_url  varchar(300)                            comment '图片路径'
) engine = InnoDB auto_increment = 1 comment = '商品图片表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for spu_info
-- -------------------------------------------------------------------------------------------------
drop table if exists spu_info;
create table if not exists spu_info
(
    id           bigint(20)    primary key auto_increment comment '商品id',
    spu_name     varchar(200)                             comment '商品名称',
    description  varchar(1000)                            comment '商品描述(后台简述）',
    category3_id bigint(20)                               comment '三级分类id',
    tm_id        bigint(20)                               comment '品牌id'
) engine = InnoDB auto_increment = 1 comment = '商品表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for spu_poster
-- -------------------------------------------------------------------------------------------------
drop table if exists spu_poster;
create table if not exists spu_poster
(
    id          bigint(20)   primary key auto_increment comment '编号',
    spu_id      bigint(20)                              comment '商品id',
    img_name    varchar(200)                            comment '文件名称',
    img_url     varchar(200)                            comment '文件路径',
    create_time datetime(0)  not null                   comment '创建时间',
    update_time datetime(0)  not null                   comment '更新时间',
    is_deleted  tinyint(3)   not null default 0         comment '逻辑删除 1（true）已删除， 0（false）未删除'
) engine = InnoDB auto_increment = 1 comment = '商品海报表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for spu_sale_attr
-- -------------------------------------------------------------------------------------------------
drop table if exists spu_sale_attr;
create table if not exists spu_sale_attr
(
    id                bigint(20)  primary key auto_increment comment '编号(业务中无关联)',
    spu_id            bigint(20)                             comment '商品id',
    base_sale_attr_id bigint(20)                             comment '销售属性id',
    sale_attr_name    varchar(20)                            comment '销售属性名称(冗余)'
) engine = InnoDB auto_increment = 1 comment = 'spu销售属性';

-- -------------------------------------------------------------------------------------------------
-- Table structure for spu_sale_attr_value
-- -------------------------------------------------------------------------------------------------
drop table if exists spu_sale_attr_value;
create table if not exists spu_sale_attr_value
(
    id                   bigint(20)  primary key auto_increment comment '销售属性值编号',
    spu_id               bigint(20)                             comment '商品id',
    base_sale_attr_id    bigint(20)                             comment '销售属性id',
    sale_attr_value_name varchar(20)                            comment '销售属性值名称',
    sale_attr_name       varchar(20)                            comment '销售属性名称(冗余)'
) engine = InnoDB auto_increment = 1 comment = 'spu销售属性值';

-- -------------------------------------------------------------------------------------------------
-- Table structure for user_address
-- -------------------------------------------------------------------------------------------------
drop table if exists user_address;
create table if not exists user_address
(
    id           bigint(20) primary key auto_increment comment '编号',
    user_id      bigint(20)                            comment '用户id',
    province_id  bigint(20)                            comment '省份id',
    user_address varchar(500)                          comment '用户地址',
    consignee    varchar(40)                           comment '收件人',
    phone_num    varchar(40)                           comment '联系方式',
    is_default   varchar(1)                            comment '是否是默认'
) engine = InnoDB auto_increment = 1  comment = '用户地址表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for user_info
-- -------------------------------------------------------------------------------------------------
drop table if exists user_info;
create table if not exists user_info
(
    id           bigint(20)   primary key auto_increment comment '编号',
    login_name   varchar(200)                            comment '用户名称',
    nick_name    varchar(200)                            comment '用户昵称',
    passwd       varchar(200)                            comment '用户密码',
    name         varchar(200)                            comment '用户姓名',
    phone_num    varchar(200)                            comment '手机号',
    email        varchar(200)                            comment '邮箱',
    head_img     varchar(200)                            comment '头像',
    user_level   varchar(200)                            comment '用户级别',
    birthday     date                                    comment '用户生日',
    gender       varchar(1)                              comment '性别 M男,F女',
    create_time  datetime(0)                             comment '创建时间',
    operate_time datetime(0)                             comment '修改时间',
    status       varchar(200)                            comment '状态'
) engine = InnoDB auto_increment = 201 comment = '用户详细信息表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for ware_info
-- -------------------------------------------------------------------------------------------------
drop table if exists ware_info;
create table if not exists ware_info
(
    id        bigint(20)    primary key , 
    name      varchar(200), 
    address   varchar(200), 
    area_code varchar(20)
) engine = InnoDB;

-- -------------------------------------------------------------------------------------------------
-- Table structure for ware_order_task
-- -------------------------------------------------------------------------------------------------
drop table if exists ware_order_task;
create table if not exists ware_order_task
(
    id               bigint(20)    primary key auto_increment comment '编号',
    order_id         bigint(20)                               comment '订单编号',
    consignee        varchar(100)                             comment '收货人',
    consignee_tel    varchar(20)                              comment '收货人电话',
    delivery_address varchar(1000)                            comment '送货地址',
    order_comment    varchar(200)                             comment '订单备注',
    payment_way      varchar(2)                               comment '付款方式 1:在线付款 2:货到付款',
    task_status      varchar(20)                              comment '工作单状态',
    order_body       varchar(200)                             comment '订单描述',
    tracking_no      varchar(200)                             comment '物流单号',
    create_time      datetime(0)                              comment '创建时间',
    ware_id          bigint(20)                               comment '仓库编号',
    task_comment     varchar(500)                             comment '工作单备注'
) engine = InnoDB auto_increment = 1 comment = '库存工作单表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for ware_order_task_detail
-- -------------------------------------------------------------------------------------------------
drop table if exists ware_order_task_detail;
create table if not exists ware_order_task_detail
(
    id            bigint(20) primary key auto_increment comment '编号',
    sku_id        bigint(20)                            comment 'sku_id',
    sku_name      varchar(200)                          comment 'sku名称',
    sku_num       int(11)                               comment '购买个数',
    task_id       bigint(20)                            comment '工作单编号',
    refund_status varchar(20)
) engine = InnoDB auto_increment = 1 comment = '库存工作单明细表';

-- -------------------------------------------------------------------------------------------------
-- Table structure for ware_sku
-- -------------------------------------------------------------------------------------------------
drop table if exists ware_sku;
create table if not exists ware_sku
(
    id           bigint(20)   primary key auto_increment comment '编号',
    sku_id       bigint(20)                              comment 'skuid',
    warehouse_id bigint(20)                              comment '仓库id',
    stock        int(11)                                 comment '库存数',
    stock_name   varchar(200)                            comment '存货名称',
    stock_locked int(11)                                 comment '锁定库存数'
) engine = InnoDB auto_increment = 1 comment = 'sku与仓库关联表';

-- -------------------------------------------------------------------------------------------------
-- View structure for base_category_view
-- -------------------------------------------------------------------------------------------------
drop view if exists base_category_view;
# create algorithm = undefined sql security definer view base_category_view as
create view base_category_view as
select b3.id   as id,
       b1.id   as category1_id,
       b1.name as category1_name,
       b2.id   as category2_id,
       b2.name as category2_name,
       b3.id   as category3_id,
       b3.name as category3_name
from base_category1 as b1 join base_category2 as b2 
    on b1.id = b2.category1_id
join base_category3 as b3 
    on b2.id = b3.category2_id;

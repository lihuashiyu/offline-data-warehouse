#!/usr/bin/env bash
    
# =========================================================================================
#    FileName      ：  mysql-kafka_init.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  mysql-kafka-init.sh 被用于 ==> 该脚本的作用是初始化所有增量表，
#                                                         只需执行一次
# =========================================================================================
    
    
MAXWELL_HOME=/opt/github/maxwell                           # MaxWell 安装路径
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
DATA_BASE=at_gui_gu                                        # 需要同步的数据库
LOG_FILE="mysql-kafka-init-$(date +%F).log"                # 操作日志存储


function import_data()
{
    echo "    开始同步表： $1 ...."
    "${MAXWELL_HOME}/bin/maxwell-bootstrap" --database ${DATA_BASE} \
                                            --table "$1" \
                                            --config "${SERVICE_DIR}/config.properties" \
                                            >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1 &
}


printf "\n=================================== 运行开始 ===================================\n"
case $1 in
    cart_info)
        import_data cart_info
    ;;
    
    comment_info)
        import_data comment_info
    ;;
    
    coupon_use)
        import_data coupon_use
    ;;
    
    favor_info)
        import_data favor_info
    ;;
    
    order_detail)
        import_data order_detail
    ;;
    
    order_detail_activity)
        import_data order_detail_activity
    ;;
    
    order_detail_coupon)
        import_data order_detail_coupon
    ;;
    
    order_info)
        import_data order_info
    ;;
    
    order_refund_info)
        import_data order_refund_info
    ;;
    
    order_status_log)
        import_data order_status_log
    ;;
    
    payment_info)
        import_data payment_info
    ;;
    
    refund_payment)
        import_data refund_payment
    ;;
    
    user_info)
        import_data user_info
    ;;
    
    all)
        import_data cart_info
        import_data comment_info
        import_data coupon_use
        import_data favor_info
        import_data order_detail
        import_data order_detail_activity
        import_data order_detail_coupon
        import_data order_info
        import_data order_refund_info
        import_data order_status_log
        import_data payment_info
        import_data refund_payment
        import_data user_info
    ;;
    
    *)
        echo "    脚本可传入一个参数，使用方法：/path/$(basename $0) arg（表名）"
        echo "        +-------------------------+----------------------+ "
        echo "        |         参   数         |       表 描 述       | "
        echo "        +-------------------------+----------------------+ "
        echo "        |  cart_info              |  购物车表            | "
        echo "        |  comment_info           |  商品评论表          | "
        echo "        |  coupon_use             |  优惠券领用表        | "
        echo "        |  favor_info             |  商品收藏表          | "
        echo "        |  order_detail           |  订单明细表          | "
        echo "        |  order_detail_activity  |  订单明细活动表      | "
        echo "        |  order_detail_coupon    |  订单明细购物券表    | "
        echo "        |  order_info             |  订单表              | "
        echo "        |  order_refund_info      |  退单表              | "
        echo "        |  order_status_log       |  订单状态日志记录表  | "
        echo "        |  payment_info           |  支付信息表          | "
        echo "        |  refund_payment         |  退款信息表          | "
        echo "        |  user_info              |  用户详细信息表      | "
        echo "        |  all                    |  全部事实表          | "
        echo "        +-------------------------+----------------------+ "
esac
printf "=================================== 运行结束 ===================================\n\n"
exit 0

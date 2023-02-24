#!/usr/bin/env bash


HIVE_DATA_BASE=warehouse


if [ -n "$2" ]; then
    do_date=$2
else
    echo "请传入日期参数"
    exit
fi


printf "\n============================== 运行开始 ==============================\n"
case $1 in
    "dws_trade_province_order_1d")
        hive -e "$dws_trade_province_order_1d"
    ;;
    
    "dws_trade_user_cart_add_1d")
        hive -e "$dws_trade_user_cart_add_1d"
    ;;
    
    "dws_trade_user_order_1d")
        hive -e "$dws_trade_user_order_1d"
    ;;
    
    "dws_trade_user_order_refund_1d")
        hive -e "$dws_trade_user_order_refund_1d"
    ;;
    
    "dws_trade_user_payment_1d")
        hive -e "$dws_trade_user_payment_1d"
    ;;
    
    "dws_trade_user_sku_order_1d")
        hive -e "$dws_trade_user_sku_order_1d"
    ;;
    
    "dws_trade_user_sku_order_refund_1d")
        hive -e "$dws_trade_user_sku_order_refund_1d"
    ;;
    
    "dws_traffic_page_visitor_page_view_1d")
        hive -e "$dws_traffic_page_visitor_page_view_1d"
    ;;
    
    "dws_traffic_session_page_view_1d")
        hive -e "$dws_traffic_session_page_view_1d"
    ;;
    
    "all")
        hive -e "${dws_trade_province_order_1d}         ${dws_trade_user_cart_add_1d}  
                 ${dws_trade_user_order_1d}             ${dws_trade_user_order_refund_1d}  
                 ${dws_trade_user_payment_1d}           ${dws_trade_user_sku_order_1d}  
                 ${dws_trade_user_sku_order_refund_1d}  ${dws_traffic_page_visitor_page_view_1d}  
                 ${dws_traffic_session_page_view_1d}"
    ;;
    
esac
printf "============================== 运行结束 ==============================\n\n"

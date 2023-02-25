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
    "ads_activity_stats")
        hive -e "${ads_activity_stats}"
    ;;
    
    "ads_coupon_stats")
        hive -e "$ads_coupon_stats"
    ;;
    
    "ads_new_buyer_stats")
        hive -e "$ads_new_buyer_stats"
    ;;
    
    "ads_order_by_province")
        hive -e "$ads_order_by_province"
    ;;
    
    "ads_page_path")
        hive -e "$ads_page_path"
    ;;
    
    "ads_repeat_purchase_by_tm")
        hive -e "$ads_repeat_purchase_by_tm"
    ;;
    
    "ads_sku_cart_num_top3_by_cate")
        hive -e "$ads_sku_cart_num_top3_by_cate"
    ;;
    
    "ads_trade_stats")
        hive -e "$ads_trade_stats"
    ;;
    
    "ads_trade_stats_by_cate")
        hive -e "$ads_trade_stats_by_cate"
    ;;
    
    "ads_trade_stats_by_tm")
        hive -e "$ads_trade_stats_by_tm"
    ;;
    
    "ads_traffic_stats_by_channel")
        hive -e "$ads_traffic_stats_by_channel"
    ;;
    
    "ads_user_action")
        hive -e "$ads_user_action"
    ;;
    
    "ads_user_change")
        hive -e "$ads_user_change"
    ;;
    
    "ads_user_retention")
        hive -e "$ads_user_retention"
    ;;
    
    "ads_user_stats")
        hive -e "$ads_user_stats"
    ;;
    
    "all")
        hive -e "${ads_activity_stats}             ${ads_coupon_stats}              ${ads_new_buyer_stats}  
                 ${ads_order_by_province}          ${ads_page_path}                 ${ads_repeat_purchase_by_tm}  
                 ${ads_sku_cart_num_top3_by_cate}  ${ads_trade_stats}               ${ads_trade_stats_by_cate}  
                 ${ads_trade_stats_by_tm}          ${ads_traffic_stats_by_channel}  ${ads_user_action}  
                 ${ads_user_change}                ${ads_user_retention}            ${ads_user_stats}"
    ;;
esac
printf "============================== 运行结束 ==============================\n\n"

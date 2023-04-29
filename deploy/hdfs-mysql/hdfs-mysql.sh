#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  hdfs-mysql.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  hdfs-mysql.sh 被用于 ==> 将 ADS 层数据导出到 Mysql  
# =========================================================================================
    
    
DATAX_DIR=/opt/github/datax                                # Datax 安装路径
HADOOP_DIR=/opt/apache/hadoop                              # Hadoop 路径
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
WAREHOUSE_DIR=/warehouse/ads                               # HDFS 的路径
MYSQL_DATA_BASE=view_report                                # Mysql 数据库名
LOG_FILE="hdfs-mysql-$(date +%F).log"                      # 操作日志


# DataX 导出路径不允许存在空文件，该函数作用为清理空文件
function export_data()
{
    # 递归查询输入路径下的所有文件
    data_dir_list=$("${HADOOP_DIR}/bin/hadoop" fs -ls -R "$2" | awk '{print $8}')
    for path in ${data_dir_list}
    do
        # DataX 导出路径不允许存在空文件，该函数作用为清理空文件
        "${HADOOP_DIR}/bin/hadoop" fs -test -z "${path}"
        if [[ $? -eq 0 ]]; then
            echo "    路径（${path}）中的文件大小为 0，正在删除 ...... "
            "${HADOOP_DIR}/bin/hadoop" fs -rm -r -f "${path}"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
        fi
    done
    
    # 执行计划
    echo "    DataX 正在同步 HDFS（$2）的文件到 Mysql 表（${MYSQL_DATA_BASE}.$(echo $1 | awk -F '/' '{print $NF}' | awk -F '.' '{print $1}')） ...... "
    /usr/bin/env python3 "${DATAX_DIR}/bin/datax.py" -p "-Dexportdir=$2" "$1"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}


printf "\n=================================== 运行开始 ===================================\n"
case $1 in
    # 新增交易用户统计
    ads_new_buyer_stats)
        export_data "${SERVICE_DIR}/conf/ads_new_buyer_stats.json" "${WAREHOUSE_DIR}/ads_new_buyer_stats"
    ;;
    
    # 各省份交易统计
    ads_order_by_province)
        export_data "${SERVICE_DIR}/conf/ads_order_by_province.json" "${WAREHOUSE_DIR}/ads_order_by_province"
    ;;
    
    # 路径分析(页面单跳)
    ads_page_path)
        export_data "${SERVICE_DIR}/conf/ads_page_path.json" "${WAREHOUSE_DIR}/ads_page_path"
    ;;
    
    # 最近7/30日各品牌复购率 
    ads_repeat_purchase_by_tm)
        export_data "${SERVICE_DIR}/conf/ads_repeat_purchase_by_tm.json" "${WAREHOUSE_DIR}/ads_repeat_purchase_by_tm"
    ;;
    
    # 交易综合统计  
    ads_trade_stats)
        export_data "${SERVICE_DIR}/conf/ads_trade_stats.json" "${WAREHOUSE_DIR}/ads_trade_stats"
    ;;
    
    # 各品类商品交易统计
    ads_trade_stats_by_cate)
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_cate.json" "${WAREHOUSE_DIR}/ads_trade_stats_by_cate"
    ;;
    
    # 各品牌商品交易统计
    ads_trade_stats_by_tm)
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_tm.json" "${WAREHOUSE_DIR}/ads_trade_stats_by_tm"
    ;;
    
    # 各渠道流量统计
    ads_traffic_stats_by_channel)
        export_data "${SERVICE_DIR}/conf/ads_traffic_stats_by_channel.json" "${WAREHOUSE_DIR}/ads_traffic_stats_by_channel"
    ;;
    
    # 用户行为漏斗分析
    ads_user_action)
        export_data "${SERVICE_DIR}/conf/ads_user_action.json" "${WAREHOUSE_DIR}/ads_user_action"
    ;;
    
    # 用户变动统计
    ads_user_change)
        export_data "${SERVICE_DIR}/conf/ads_user_change.json" "${WAREHOUSE_DIR}/ads_user_change"
    ;;
    
    # 用户留存率
    ads_user_retention)
        export_data "${SERVICE_DIR}/conf/ads_user_retention.json" "${WAREHOUSE_DIR}/ads_user_retention"
    ;;
    
    # 用户新增活跃统计
    ads_user_stats)
        export_data "${SERVICE_DIR}/conf/ads_user_stats.json" "${WAREHOUSE_DIR}/ads_user_stats"
    ;;
    
    # 最近 30 天发布的活动的补贴率
    ads_activity_stats)
        export_data "${SERVICE_DIR}/conf/ads_activity_stats.json" "${WAREHOUSE_DIR}/ads_activity_stats"
    ;;
    
    # 最近 30 天发布的优惠券的补贴率
    ads_coupon_stats)
        export_data "${SERVICE_DIR}/conf/ads_coupon_stats.json" "${WAREHOUSE_DIR}/ads_coupon_stats"
    ;;
    
    # 各分类商品购物车存量 top3
    ads_sku_cart_num_top3_by_cate)
        export_data "${SERVICE_DIR}/conf/ads_sku_cart_num_top3_by_cate.json" "${WAREHOUSE_DIR}/ads_sku_cart_num_top3_by_cate"
    ;;
    
    all)
        export_data "${SERVICE_DIR}/conf/ads_new_buyer_stats.json"           "${WAREHOUSE_DIR}/ads_new_buyer_stats"
        export_data "${SERVICE_DIR}/conf/ads_order_by_province.json"         "${WAREHOUSE_DIR}/ads_order_by_province"
        export_data "${SERVICE_DIR}/conf/ads_page_path.json"                 "${WAREHOUSE_DIR}/ads_page_path"
        export_data "${SERVICE_DIR}/conf/ads_repeat_purchase_by_tm.json"     "${WAREHOUSE_DIR}/ads_repeat_purchase_by_tm"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats.json"               "${WAREHOUSE_DIR}/ads_trade_stats"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_cate.json"       "${WAREHOUSE_DIR}/ads_trade_stats_by_cate"
        export_data "${SERVICE_DIR}/conf/ads_trade_stats_by_tm.json"         "${WAREHOUSE_DIR}/ads_trade_stats_by_tm"
        export_data "${SERVICE_DIR}/conf/ads_traffic_stats_by_channel.json"  "${WAREHOUSE_DIR}/ads_traffic_stats_by_channel"
        export_data "${SERVICE_DIR}/conf/ads_user_action.json"               "${WAREHOUSE_DIR}/ads_user_action"
        export_data "${SERVICE_DIR}/conf/ads_user_change.json"               "${WAREHOUSE_DIR}/ads_user_change"
        export_data "${SERVICE_DIR}/conf/ads_user_retention.json"            "${WAREHOUSE_DIR}/ads_user_retention"
        export_data "${SERVICE_DIR}/conf/ads_user_stats.json"                "${WAREHOUSE_DIR}/ads_user_stats"
        export_data "${SERVICE_DIR}/conf/ads_activity_stats.json"            "${WAREHOUSE_DIR}/ads_activity_stats"
        export_data "${SERVICE_DIR}/conf/ads_coupon_stats.json"              "${WAREHOUSE_DIR}/ads_coupon_stats"
        export_data "${SERVICE_DIR}/conf/ads_sku_cart_num_top3_by_cate.json" "${WAREHOUSE_DIR}/ads_sku_cart_num_top3_by_cate"
    ;;
    
    *)
        export_data "${SERVICE_DIR}/conf/ads_new_buyer_stats.json"           "${WAREHOUSE_DIR}/ads_new_buyer_stats"
        echo "    脚本可传入一个参数，如下所示：                                       "
        echo "        +-------------------------------+--------------------------------+ "
        echo "        |             参 数             |             描  述             | "
        echo "        +-------------------------------+--------------------------------+ "
        echo "        | ads_activity_stats            | 最近 30 天发布的活动的补贴率   | "
        echo "        | ads_coupon_stats              | 最近 30 天发布的优惠券的补贴率 | "
        echo "        | ads_new_buyer_stats           | 新增交易用户统计               | "
        echo "        | ads_order_by_province         | 各省份交易统计                 | "
        echo "        | ads_page_path                 | 路径分析(页面单跳)             | "
        echo "        | ads_repeat_purchase_by_tm     | 最近 7/30 日各品牌复购率       | "
        echo "        | ads_sku_cart_num_top3_by_cate | 各分类商品购物车存量 Top3      | "
        echo "        | ads_trade_stats_by_cate       | 各品类商品交易统计             | "
        echo "        | ads_trade_stats_by_tm         | 各品牌商品交易统计             | "
        echo "        | ads_trade_stats               | 交易综合统计                   | "
        echo "        | ads_traffic_stats_by_channel  | 各渠道流量统计                 | "
        echo "        | ads_user_action               | 用户行为漏斗分析               | "
        echo "        | ads_user_change               | 用户变动统计                   | "
        echo "        | ads_user_retention            | 用户留存率                     | "
        echo "        | ads_user_stats                | 用户新增活跃统计               | "
        echo "        | all                           | 全部 ads 表                    | "   
        echo "        +-------------------------------+--------------------------------+ "
    ;;
esac
printf "=================================== 运行结束 ===================================\n\n"
exit 0

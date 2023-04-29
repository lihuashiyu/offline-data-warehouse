#!/usr/bin/env bash
    
# =========================================================================================
#    FileName      ：  mysql-hdfs.sh
#    CreateTime    ：  2023-02-24 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  mysql-hdfs.sh 被用于 ==> DataX 将生成的 业务数据 全量同步到 HDFS，
#                                                   仅在项目部署完成后，初始化时使用，
#                                                   后续无需再次操作
# =========================================================================================
    
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 程序位置
DATAX_HOME=/opt/github/datax                               # Datax 安装路径
HADOOP_HOME=/opt/apache/hadoop                             # Hadoop 安装路径
WAREHOUSE_DIR=/warehouse/db                                # HDFS 上的路径
LOG_FILE="mysql-hdfs-$(date +%F).log"                      # 操作日志
MYSQL_DATA_BASE="at_gui_gu"                                # Mysql 的数据库名称
do_date=$(date -d "-1 day" +%F)                            # 当前日期的前一天

# 如果传入日期则 do_date 等于传入的日期，否则等于前一天日期
if [ -n "$2" ]; then
    do_date=$2
fi


# 数据同步
import_data()
{
    # 1. 判断路径是否存在
    ${HADOOP_HOME}/bin/hadoop fs -test -e "$2"
    
    # 2. 处理目标路径：如果目标路径不存在，则创建；若存在，则清空，目的是保证同步任务可重复执行
    if [[ $? -eq 1 ]]; then
        echo "    路径（$2）不存在，正在创建 ...... "
        ${HADOOP_HOME}/bin/hadoop fs -mkdir -p "$2"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    else
        cs=$("${HADOOP_HOME}/bin/hadoop" fs -count "$2" | awk '{print $3}')
        if [[ "${cs}" -ne 0 ]]; then
            echo "    路径（$2）不为空，正在清空 ...... "
            ${HADOOP_HOME}/bin/hadoop fs -rm -r -f "$2/*"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
        fi
    fi
    
    # 3. 执行计划，进行数据同步
    echo "    DataX 正在同步表（${MYSQL_DATA_BASE}.$(echo $1 | awk -F '/' '{print $4}' | awk -F '.' '{print $1}')）的数据到 HDFS（$2）路径 ...... "
    /usr/bin/env python3 "${DATAX_HOME}/bin/datax.py" -p "-Dtargetdir=$2" "$1"  >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}


printf "\n=================================== 运行开始 ===================================\n"
# 导出数据
case $1 in
    # 活动信息表
    activity_info)
        import_data "${SERVICE_DIR}/conf/activity_info.json" "${WAREHOUSE_DIR}/activity_info/${do_date}"
    ;;
    
    # 活动规则表
    activity_rule)
        import_data "${SERVICE_DIR}/conf/activity_rule.json" "${WAREHOUSE_DIR}/activity_rule_full/${do_date}"
    ;;
       
    # 一级分类表
    base_category1)
        import_data "${SERVICE_DIR}/conf/base_category1.json" "${WAREHOUSE_DIR}/base_category1_full/${do_date}"
    ;;
    
    # 二级分类表    
    base_category2)
        import_data "${SERVICE_DIR}/conf/base_category2.json" "${WAREHOUSE_DIR}/base_category2_full/${do_date}"
    ;;
    
    # 三级分类表    
    base_category3)
        import_data "${SERVICE_DIR}/conf/base_category3.json" "${WAREHOUSE_DIR}/base_category3_full/${do_date}"
    ;;
    
    # 字典表    
    base_dic)
        import_data "${SERVICE_DIR}/conf/base_dic.json" "${WAREHOUSE_DIR}/base_dic_full/${do_date}"
    ;;
       
    # 省份表 
    base_province)
        import_data "${SERVICE_DIR}/conf/base_province.json" "${WAREHOUSE_DIR}/base_province_full/${do_date}"
    ;;
        
    # 地区表
    base_region)
        import_data "${SERVICE_DIR}/conf/base_region.json" "${WAREHOUSE_DIR}/base_region_full/${do_date}"
    ;;
        
    # 品牌表
    base_trademark)
        import_data "${SERVICE_DIR}/conf/base_trademark.json" "${WAREHOUSE_DIR}/base_trademark_full/${do_date}"
    ;;        
      
    # 购物车表 
    cart_info)
        import_data "${SERVICE_DIR}/conf/cart_info.json" "${WAREHOUSE_DIR}/cart_info_full/${do_date}"
    ;;
        
    # 优惠券信息
    coupon_info)
        import_data "${SERVICE_DIR}/conf/coupon_info.json" "${WAREHOUSE_DIR}/coupon_info_full/${do_date}"
    ;;
        
    # SKU 平台属性值表
    sku_attr_value)
        import_data "${SERVICE_DIR}/conf/sku_attr_value.json" "${WAREHOUSE_DIR}/sku_attr_value_full/${do_date}"
    ;;
    
    # SKU 信息表
    sku_info)
        import_data "${SERVICE_DIR}/conf/sku_info.json" "${WAREHOUSE_DIR}/sku_info_full/${do_date}"
    ;;
        
    # SKU 销售属性表
    sku_sale_attr_value)
        import_data "${SERVICE_DIR}/conf/sku_sale_attr_value.json" "${WAREHOUSE_DIR}/sku_sale_attr_value_full/${do_date}"
    ;;
       
    # SPU 信息表 
    spu_info)
        import_data "${SERVICE_DIR}/conf/spu_info.json" "${WAREHOUSE_DIR}/spu_info_full/${do_date}"
    ;;
    
    # 全部 15 张表    
    all)
        import_data "${SERVICE_DIR}/conf/activity_info.json"       "${WAREHOUSE_DIR}/activity_info_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/activity_rule.json"       "${WAREHOUSE_DIR}/activity_rule_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_category1.json"      "${WAREHOUSE_DIR}/base_category1_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_category2.json"      "${WAREHOUSE_DIR}/base_category2_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_category3.json"      "${WAREHOUSE_DIR}/base_category3_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_dic.json"            "${WAREHOUSE_DIR}/base_dic_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_province.json"       "${WAREHOUSE_DIR}/base_province_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_region.json"         "${WAREHOUSE_DIR}/base_region_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/base_trademark.json"      "${WAREHOUSE_DIR}/base_trademark_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/cart_info.json"           "${WAREHOUSE_DIR}/cart_info_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/coupon_info.json"         "${WAREHOUSE_DIR}/coupon_info_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/sku_attr_value.json"      "${WAREHOUSE_DIR}/sku_attr_value_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/sku_info.json"            "${WAREHOUSE_DIR}/sku_info_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/sku_sale_attr_value.json" "${WAREHOUSE_DIR}/sku_sale_attr_value_full/${do_date}"
        import_data "${SERVICE_DIR}/conf/spu_info.json"            "${WAREHOUSE_DIR}/spu_info_full/${do_date}"
    ;;
    
    *)
        echo "    脚本可传入一个参数，如下所示：                    "
        echo "        +----------------------+------------------+ "
        echo "        |        参  数        |      描  述      | "
        echo "        +----------------------+------------------+ "
        echo "        | activity_info        | 活动信息表       | "   
        echo "        | activity_rule        | 活动规则表       | "     
        echo "        | base_category1       | 一级分类表       | "     
        echo "        | base_category2       | 二级分类表       | "     
        echo "        | base_category3       | 三级分类表       | "     
        echo "        | base_dic             | 字典表           | "     
        echo "        | base_province        | 省份表           | "       
        echo "        | base_region          | 地区表           | "       
        echo "        | base_trademark       | 品牌表           | "       
        echo "        | cart_info            | 购物车表         | "      
        echo "        | coupon_info          | 优惠券信息       | "     
        echo "        | sku_attr_value       | SKU 平台属性值表 | " 
        echo "        | sku_info             | SKU 信息表       | "   
        echo "        | sku_sale_attr_value  | SKU 销售属性表   | " 
        echo "        | spu_info             | SPU 信息表       | "   
        echo "        | all                  | 全部 mysql 表    | "   
        echo "        +----------------------+------------------+ "
    ;;
esac
printf "=================================== 运行结束 ===================================\n\n"
exit 0

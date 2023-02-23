#!/usr/bin/env bash


HIVE_DB=gmall                                              # 数仓 DB 名称
HIVE_ENGINE=hive                                           # 查询引擎
MYSQL_HOST=issac                                           # Mysql 主机地址
MYSQL_PORT=3306                                            # Mysql 端口号
MYSQL_USER=issac                                           # Mysql 用户名
MYSQL_PASSWD=111111                                        # Mysql 密码
MYSQL_DB=data_supervisor                                   # Mysql 数据库名
MYSQL_TABLE=week_on_week                                   # Mysql 表名称


# 计算一张表一周数据量同比增长值
while getopts "t:d:s:x:l:" arg;                            # 参数解析
do
    case $arg in
        t)                                                 # 要处理的表名
            TABLE=${OPTARG}
            ;;
        d)                                                 # 日期
            DT=${OPTARG}
            ;;
        s)                                                 # 同比增长指标下限
            MIN=${OPTARG}
            ;;
        x)                                                 # 同比增长指标上限
            MAX=${OPTARG}
            ;;
        l)                                                 # 告警级别
            LEVEL=${OPTARG}
            ;;
        ?)
            echo "未知参数，参数可选择：| t | d | s | x | l |"
            exit 1
            ;;
    esac
done


# 如果 dt 和 level 没有设置，那么默认值 dt 是昨天 告警级别是 0
[ "${DT}" ] || DT=$(date -d '-1 day' +%F)
[ "${LEVEL}" ] || LEVEL=0


# 认证为 hive 用户，如在非安全(Hadoop 未启用 Kerberos 认证)环境中，则无需认证
kinit -kt /etc/security/keytab/hive.keytab hive

# 上周数据量
LAST_WEEK=$(${HIVE_ENGINE} -e "set hive.cli.print.header=false; select count(1) from ${HIVE_DB}.${TABLE} where dt=date_add('${DT}',-7);")

# 本周数据量
THIS_WEEK=$(${HIVE_ENGINE} -e "set hive.cli.print.header=false; select count(1) from ${HIVE_DB}.${TABLE} where dt = '${DT}';")

# 计算增长
if [ "${LAST_WEEK}" -ne 0 ]; then
    RESULT=$(awk "BEGIN{print (${THIS_WEEK}-${LAST_WEEK})/${LAST_WEEK}*100}")
else
    RESULT=10000
fi

# 将结果写入MySQL
mysql -h"${MYSQL_HOST}" -P"$MYSQL_PORT" -u"${MYSQL_USER}" -p"${MYSQL_PASSWD}" \
      -e"insert into ${MYSQL_DB}.${MYSQL_TABLE} values('${DT}', '${TABLE}', ${RESULT}, ${MIN}, ${MAX}, ${LEVEL})
         on duplicate key update \`value\` = ${RESULT}, value_min = ${MIN}, value_max = ${MAX}, notification_level = ${LEVEL};"

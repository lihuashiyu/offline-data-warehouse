#!/usr/bin/env bash


HIVE_DB=gmall                                              # 数仓 DB 名称
HIVE_ENGINE=hive                                           # 查询引擎
MYSQL_HOST=issac                                           # Mysql 主机地址
MYSQL_PORT=3306                                            # Mysql 端口号
MYSQL_USER=issac                                           # Mysql 用户名
MYSQL_PASSWD=111111                                        # Mysql 密码
MYSQL_DB=data_supervisor                                   # Mysql 数据库名
MYSQL_TABLE=day_on_day                                     # Mysql 表名称


# 计算一张表单日数据量环比增长值
while getopts "t:d:s:x:l:" arg                             # 参数解析 
do
    case $arg in                                           # 要处理的表名
        t)
            TABLE=${OPT_ARG}
            ;;
        d)                                                 # 日期
            DT=${OPT_ARG}
            ;;
        s)                                                 # 环比增长指标下限
            MIN=${OPT_ARG}
            ;;
        x)                                                 # 环比增长指标上限
            MAX=${OPT_ARG}
            ;;
        l)                                                 # 告警级别
            LEVEL=${OPT_ARG}
            ;;
        ?)
            echo "未知参数"
            exit 1
            ;;
    esac
done

# 如果 dt 和 level 没有设置，那么默认值 dt 是昨天 告警级别是 0
[ "${DT}" ] || DT=$(date -d '-1 day' +%F)
[ "${LEVEL}" ] || LEVEL=0


# 认证为hive用户，如在非安全(Hadoop未启用Kerberos认证)环境中，则无需认证
kinit -kt /etc/security/keytab/hive.keytab hive

# 昨日数据量
YESTERDAY=$(${HIVE_ENGINE} -e "set hive.cli.print.header=false; select count(1) from ${HIVE_DB}.${TABLE} where dt=date_add('${DT}',-1);")

# 今日数据量
TODAY=$(${HIVE_ENGINE} -e "set hive.cli.print.header=false;select count(1) from ${HIVE_DB}.${TABLE} where dt='${DT}';")

# 计算环比增长值
if [ "${YESTERDAY}" -ne 0 ]; then
    RESULT=$(awk "BEGIN{print (${TODAY}-${YESTERDAY})/${YESTERDAY}*100}")
else
    RESULT=10000
fi

# 将结果写入MySQL表格
mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWD}" \
      -e"insert into ${MYSQL_DB}.${MYSQL_TABLE} values ('${DT}', '${TABLE}', ${RESULT}, ${MIN}, ${MAX}, ${LEVEL})
         on duplicate key update \`value\`=${RESULT}, value_min=${MIN}, value_max=${MAX}, notification_level=${LEVEL};"

#!/usr/bin/env bash


HIVE_DB=gmall                                              # 数仓 DB 名称
HIVE_ENGINE=hive                                           # 查询引擎
MYSQL_HOST=issac                                           # Mysql 主机地址
MYSQL_PORT=3306                                            # Mysql 端口号
MYSQL_USER=issac                                           # Mysql 用户名
MYSQL_PASSWD=111111                                        # Mysql 密码
MYSQL_DB=data_supervisor                                   # Mysql 数据库名
MYSQL_TABLE=rng                                            # Mysql 表名称


# 计算某一列异常值个数
while getopts "t:d:l:c:s:x:a:b:" arg; do
    case $arg in
        t)                                                 # 要处理的表名
            TABLE=${OPT_ARG}
            ;;
        d)                                                 # 日替
            DT=${OPT_ARG}
            ;;
        c)                                                 # 要处理的列
            COL=${OPT_ARG}
            ;;
        s)                                                 # 不在规定值域的值的个数下限
            MIN=${OPT_ARG}
            ;;
        x)                                                 # 不在规定值域的值的个数上限
            MAX=${OPT_ARG}
            ;;
        l)                                                 # 告警级别
            LEVEL=${OPT_ARG}
            ;;
        a)                                                 # 规定值域为a-b
            RANGE_MIN=${OPT_ARG}
            ;;
        b)
            RANGE_MAX=${OPT_ARG}
            ;;
        ?)
            echo "未知参数，参数可选择：| t | d | | c | s | x | l | a | b |"
            exit 1
            ;;
    esac
done

# 如果 dt 和 level 没有设置，那么默认值 dt 是昨天 告警级别是 0
[ "${DT}" ] || DT=$(date -d '-1 day' +%F)
[ "${LEVEL}" ] || LEVEL=0


# 认证为hive用户，如在非安全(Hadoop未启用Kerberos认证)环境中，则无需认证
kinit -kt /etc/security/keytab/hive.keytab hive

# 查询不在规定值域的值的个数
RESULT=$(${HIVE_ENGINE} -e "set hive.cli.print.header=false; select count(1) from ${HIVE_DB}.${TABLE} where dt='${DT}' and ${COL} not between ${RANGE_MIN} and ${RANGE_MAX};")

# 将结果写入MySQL
mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWD}" \
      -e"insert into ${MYSQL_DB}.${MYSQL_TABLE} values ('${DT}', '${TABLE}', '${COL}', ${RESULT}, ${RANGE_MIN}, ${RANGE_MAX}, ${MIN}, ${MAX}, ${LEVEL})
         on duplicate key update \`value\` = ${RESULT}, range_min = ${RANGE_MIN}, range_max = ${RANGE_MAX}, value_min = ${MIN}, value_max=${MAX}, notification_level=${LEVEL};"

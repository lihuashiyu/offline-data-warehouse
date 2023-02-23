#!/usr/bin/env bash


HIVE_DB=gmall                                              # 数仓 DB 名称
HIVE_ENGINE=hive                                           # 查询引擎
MYSQL_HOST=issac                                           # Mysql 主机地址
MYSQL_PORT=3306                                            # Mysql 端口号
MYSQL_USER=issac                                           # Mysql 用户名
MYSQL_PASSWD=111111                                        # Mysql 密码
MYSQL_DB=data_supervisor                                   # Mysql 数据库名
MYSQL_TABLE=day_on_day                                     # Mysql 表名称


# 监控某张表一列的重复值
while getopts "t:d:c:s:x:l:" arg; do                       # 参数解析
    case $arg in                                           # 要处理的表名
        t)
            TABLE=${OPT_ARG}
            ;;
        d)                                                 # 日期
            DT=${OPT_ARG}
            ;;
        c)                                                 # 要计算重复值的列名
            COL=${OPT_ARG}
            ;;
        s)                                                 # 重复值指标下限
            MIN=${OPT_ARG}
            ;;
        x)                                                 # 重复值指标上限
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

# 如果 dt 和 level 没有设置，那么默认值 d t是昨天 告警级别是 0
[ "${DT}" ] || DT=$(date -d '-1 day' +%F)
[ "${LEVEL}" ] || LEVEL=0


# 认证为 hive 用户，如在非安全(Hadoop 未启用 Kerberos 认证)环境中，则无需认证
kinit -kt /etc/security/keytab/hive.keytab hive

# 重复值个数
RESULT=$($HIVE_ENGINE -e "set hive.cli.print.header=false;select count(1) from (select ${COL} from ${HIVE_DB}.${TABLE} where dt='${DT}' group by ${COL} having count(${COL})>1) t1;")

# 将结果插入MySQL
mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWD}" \
      -e"insert into ${MYSQL_DB}.${MYSQL_TABLE} values ('${DT}', '${TABLE}', '${COL}', ${RESULT}, ${MIN}, ${MAX}, ${LEVEL})
       on duplicate key update \`value\`=${RESULT}, value_min=${MIN}, value_max=${MAX}, notification_level=${LEVEL};"

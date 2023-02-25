#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
====================================================================================================
    ProjectName   ：  Sql 
    FileName      ：  GenerateImportDatacConfig 
    CreateTime    ：  2022/7/31 15:40:36 
    Author        ：  lihua shiyu 
    Email         ：  lihuashiyu@github.com 
    PythonCompiler：  3.9.13 
    IDE           ：  PyCharm 2020.3.4 
    Version       ：  1.0 
    Description   ：  文件描述 
====================================================================================================
"""

import os
import MySQLdb


# MySQL 相关配置，需根据实际情况作出修改
mysql_host = "master"
mysql_port = "3306"
mysql_user = "issac"
mysql_passwd = "111111"

# HDFS NameNode相关配置，需根据实际情况作出修改
hdfs_nn_host = "issac"
hdfs_nn_port = "9000"


# 获取 mysql 连接
def get_connection():
    return MySQLdb.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, passwd=mysql_passwd)


# 获取表格的元数据  包含列名和数据类型
def get_mysql_meta(database, table):
    connection = get_connection()
    cursor = connection.cursor()
    sql = "select column_name,data_type from information_schema.columns where table_schema=%s " \
          "and table_name=%s order by ordinal_position"
    cursor.execute(sql, [database, table])
    fetchall = cursor.fetchall()
    cursor.close()
    connection.close()
    return fetchall


# 获取 mysql 表的列名
def get_mysql_columns(database, table):
    field_type_tuple = get_mysql_meta(database, table)
    
    column_list = []
    for field_type in field_type_tuple:
        column_list.append(field_type[0])
    return column_list


# 将获取的元数据中mysql的数据类型转换为hive的数据类型  写入到hdfswriter中
def get_hive_columns(database, table):
    mappings = {
        "bigint": "bigint",
        "int": "bigint",
        "smallint": "bigint",
        "tinyint": "bigint",
        "decimal": "string",
        "double": "double",
        "float": "float",
        "binary": "string",
        "char": "string",
        "varchar": "string",
        "datetime": "string",
        "time": "string",
        "timestamp": "string",
        "date": "string",
        "text": "string"
    }
    
    meta_tupple = get_mysql_meta(database, table)
    
    field_type_list = []
    for meat in meta_tupple:
        field_type_list.append({"name": meat[0], "type": mappings[meat[1]].lower()})
    return field_type_list


# 生成 json 文件
def generate_json(source_database, source_table):
    job = {
        "job": {
            "setting": {
                "speed": {
                    "channel": 3
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            },
            "content": [{
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": mysql_user,
                        "password": mysql_passwd,
                        "column": get_mysql_columns(source_database, source_table),
                        "splitPk": "",
                        "connection": [{
                            "table": [source_table],
                            "jdbcUrl": ["jdbc:mysql://" + mysql_host + ":" + mysql_port + "/" + source_database]
                        }]
                    }
                },
                "writer": {
                    "name": "hdfswriter",
                    "parameter": {
                        "defaultFS": "hdfs://" + hdfs_nn_host + ":" + hdfs_nn_port,
                        "fileType": "text",
                        "path": "${targetdir}",
                        "fileName": source_table,
                        "column": get_hive_columns(source_database, source_table),
                        "writeMode": "append",
                        "fieldDelimiter": "\t",
                        "compress": "gzip"
                    }
                }
            }]
        }
    }
    
    output_path = f"./{source_table}.json" 
    with open(output_path, "w") as f:
        f.write(str(job).replace("'", "\""))


if __name__ == '__main__':
    table_list = ["activity_info", "activity_rule", "base_category1", "base_category2", "base_category3",
                  "base_dic", "base_province", "base_region", "base_trademark", "cart_info",
                  "coupon_info", "sku_attr_value", "sku_info", "sku_sale_attr_value", "spu_info"]
    
    for table in table_list:
        generate_json(source_database="at_gui_gu", source_table=table)

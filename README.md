# <center>**offline-data-warehouse**</center>

## 1. 项目总体结构

本项目基于 [**B站**](https://www.bilibili.com/) 上 [**尚硅谷离线数仓 5.0**](https://www.bilibili.com/video/BV1AT411j7hu/?vd_source=4840054c2f70d7736de5d9da8bed9fb2) 进行学习后的笔记整理，总体的目录结构如下：

```bash
    offline-data-warehouse
     ├── deploy                                            # 各个部署模块
     │     ├── file-kafka                                  # 使用 flume 监控【用户行为日志】，并同步到 kafka
     │     ├── hdfs-mysql                                  # 使用 datax 将 ads 层的数据同步到 mysql
     │     ├── kafka-hdfs-db                               # 使用 flume 将 kafka 中的业务数据，同步到 hdfs
     │     ├── kafka-hdfs-log                              # 使用 flume 将 kafka 中日志数据，同步到 hdfs
     │     ├── mock-db                                     # 模仿【用户行为】日志生成模块
     │     ├── mock-log                                    # 模仿【业务数据】生成模块
     │     ├── mysql-hdfs                                  # 使用 datax 将 MySQL 的全量数据，同步到 hdfs
     │     ├── mysql-kafka                                 # 使用 maxwell 监控 MySQL 增量数据，并同步到 kafka
     │     ├── sql                                         # 整个离线数仓使用到的 mysql-sql，hive-sql，doris-sql
     │     └── warehouse                                   # 数仓各层之间的调用脚本
     ├── deploy.sh                                         # 一键打包脚本
     ├── doc                                               # 尚硅谷相关文档
     ├── flume                                             # 自定义 flume 拦截器源码，用于拦截不规则的数据
     └── README.md                                         # 项目说明文档                                                                
```


<br/>

## 2. 项目架构图


![项目架构图](doc/5-%E9%87%87%E9%9B%865.0%E6%9E%B6%E6%9E%84.png)

<br/>

![项目部署图](doc/5-%E9%87%87%E9%9B%865.0%E6%9E%B6%E6%9E%84.png)

<br/>

## 3. 项目模块说明

### 3.1 mock-log 模块

```bash
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 3.3 章节
    mock-log/
     ├── application.yml                                   # mock-log 的配置文件                                       
     ├── cycle.sh                                          # 该脚本调用 mock-log.sh，进行循环生成，默认 10 次
     ├── log                                               # 运行产生的日志目录
     ├── logback.xml                                       # 日志配置文件
     ├── mock-log.jar                                      # 执行的 jar
     ├── mock-log.sh                                       # mock-log 模块的启停脚本
     └── path.json                                         # 与生成的数据相关 
```

### 3.2 file-kafka 模块

```bash
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 4.3 章节
    file-kafka/
     ├── position.json                                     # flume 监控本地文件产生的记录
     ├── file-kafka.conf                                   # flume 监控本地文件的配置文件
     ├── file-kafka.sh                                     # 启停脚本
     └── flume-1.0.jar                                     # 执行 offline-data-warehouse/flume/build.sh 生成的 jar，用于拦截不规则数据
```

### 3.3 mock-db 模块

```bash
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.2 章节
    mock-db/
     ├── application.properties                            # mock-db 的配置文件
     ├── cycle.sh                                          # 该脚本调用 mock-log.sh，进行循环生成，默认 10 次
     ├── data.sql                                          # 模拟 mysql 数据库原始的数据
     ├── mock-db.jar                                       # 执行的 jar，修改过尚硅谷原始的 jar，已经支持 mysql 8.0.x
     ├── mock-db.sh                                        # mock-db 模块的启停脚本
     └── table.sql                                         # 建表语句 
```

### 3.4 mysql-hdfs 模块

```bash
    # 使用 DataX 将表中的数据全量同步到 HDFS，仅在项目部署的时候使用一次，后续无需再次操作，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mysql-hdfs
     ├── activity_info.json
     ├── activity_rule.json
     ├── base_category1.json
     ├── base_category2.json
     ├── base_category3.json
     ├── base_dic.json
     ├── base_province.json
     ├── base_region.json
     ├── base_trademark.json
     ├── cart_info.json
     ├── coupon_info.json
     ├── GenerateMysqlHdfsJob.py                           # 使用 python 生成 DataX 的 mysql --> hdfs 的配置文件
     ├── mysql_hdfs.sh                                     # 该脚本调用 DataX  mysql 数据同步到 hdfs 
     ├── sku_attr_value.json
     ├── sku_info.json
     ├── sku_sale_attr_value.json
     └── spu_info.json                                                             
```

### 3.5 mysql-kafka 模块

```bash
    # 用于监控 Mysql，当 mock-db 在 Mysql 中产生增量数据时，将增量数据同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mysql-kafka 
     ├── config.properties                                 # MaxWell 监控 Mysql 的配置文件
     ├── meta.sql                                          # MaxWell 监控时，在数据库创建的表
     ├── mysql_kafka.sh                                    # 监控数据库启停脚本 
     └── mysql_kafka_init.sh                               # 初始化所有的增量表，只需安装时执行
```

### 3.6 kafka-hdfs-log 模块

```bash
    # 用于监控 Mysql，当 mock-db 在 Mysql 中产生增量数据时，将增量数据同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    kafka-hdfs-log
     ├── data                                              # flume 同步过程中产生的数据存储目录
     ├── check-point                                       # 保存的检查点数据
     ├── kafka-hdfs-log.conf                               # 用户行为 同步到 hdfs 的配置文件
     └── kafka-hdfs-log.sh                                 # 用户行为同步启停脚本
```

### 3.7 kafka-hdfs-db 模块

```bash
    # 用于监控 Mysql，当 mock-db 在 Mysql 中产生增量数据时，将增量数据同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    kafka-hdfs-log
     ├── data                                              # flume 同步过程中产生的数据存储目录
     ├── check-point                                       # 保存的检查点数据
     ├── kafka-hdfs-log.conf                               # 业务数据 同步到 hdfs 的配置文件
     └── kafka-hdfs-log.sh                                 # 业务数据 同步启停脚本
```

### 3.8 hdfs-mysql 模块
```bash
    hdfs-mysql/
     ├── ads_activity_stats.json
     ├── ads_coupon_stats.json
     ├── ads_new_buyer_stats.json
     ├── ads_order_by_province.json
     ├── ads_page_path.json
     ├── ads_repeat_purchase_by_tm.json
     ├── ads_sku_cart_num_top3_by_cate.json
     ├── ads_trade_stats_by_cate.json
     ├── ads_trade_stats_by_tm.json
     ├── ads_trade_stats.json
     ├── ads_traffic_stats_by_channel.json
     ├── ads_user_action.json
     ├── ads_user_change.json
     ├── ads_user_retention.json
     ├── ads_user_stats.json
     ├── GenerateHdfsMysql.py                              # 使用 python 生成 DataX 的 hdfs --> mysql 的配置文件
     └── hdfs_mysql.sh                                     # 该脚本调用 DataX 将 hdfs 数据同步到 mysql

```

### 3.9 sql 模块

```bash
    # 使用 DataX 将 Hive ADS 层的表数据同步到 mysql，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    sql
     ├── ads.sql                                           # ADS 建表和插入数据用到的 hive-sql
     ├── dim.sql                                           # DIM 建表和插入数据用到的 hive-sql
     ├── dwd.sql                                           # DWD 建表和插入数据用到的 hive-sql
     ├── dws.sql                                           # DWS 建表和插入数据用到的 hive-sql
     ├── export.sql                                        # ADS 层导出到 mysql 的建表语句
     └── ods.sql                                           # ODS 建表和插入数据用到的 hive-sql
```

### 3.10 shell 模块

```bash
    # 使用 DataX 将 Hive ADS 层的表数据同步到 mysql，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    warehouse/
     ├── check_dim.sh
     ├── check_dwd.sh
     ├── check_ods.sh
     ├── command
     ├── component.sh                                      # 各个大数据组件的启停脚本
     ├── day_on_day.sh
     ├── duplicate.sh
     ├── dwd_dws_1d.sh
     ├── dwd_dws_history.sh
     ├── dwd_dws_nd.sh
     ├── dws_ads.sh
     ├── hdfs_ods_db.sh
     ├── hdfs_ods_log.sh
     ├── hdfs_to_ods_db_add.sh
     ├── hdfs_to_ods_db_init.sh
     ├── hdfs_to_ods_log.sh
     ├── hive_on_spark.sh
     ├── mysql_to_hdfs_add.sh
     ├── mysql_to_hdfs_init.sh
     ├── null_id.sh
     ├── ods_dim_init.sh
     ├── ods_dim.sh
     ├── ods_dwd_init.sh
     ├── ods_dwd.sh
     ├── ods_to_dim_db_add.sh
     ├── ods_to_dim_db_init.sh
     ├── ods_to_dwd_db_add.sh
     ├── ods_to_dwd_db_init.sh
     ├── ods_to_dwd_log.sh
     ├── one_key_init.sh                                   # 部署完成后，一键初始化
     ├── one_key_install.sh                                # 一键部署脚本
     ├── range.sh
     ├── warehouse.sh                                      # 整个数据流向数仓的启停脚本
     └── week_on_week.sh
    
```

<br/>

## 4. 基础组件的集群部署


### 4.1 大数据组件及版本说明

<center>服务器基本信息</center>

|        |      master      |     slaver1      |     slaver2      |     slaver3      |
|:------:|:----------------:|:----------------:|:----------------:|:----------------:|
|  CPU   |       4C/8T      |       4C/8T      |       4C/8T      |       4C/8T      |
|  内存  |   16GB/3200MHz   |   16GB/3200MHz   |   16GB/3200MHz   |   16GB/3200MHz   |
|  硬盘  |     HDD 40GB     |     HDD 40GB     |     HDD 40GB     |     HDD 40GB     |
|  网卡  |     1000Mbps     |     1000Mbps     |     1000Mbps     |     1000Mbps     |
|  系统  | Rocky Linux 9.1  | Rocky Linux 9.1  | Rocky Linux 9.1  | Rocky Linux 9.1  |


<br><br>
<center>集群服务器规划</center>

|     服务名称     | 版本号 |      子服务       | master | slaver1 | slaver2 | slaver3 |                              说明                              |
|:----------------:|:------:|:-----------------:|:------:|:-------:|:-------:|:-------:|:--------------------------------------------------------------:|
|                  |        |      NameNode     |   √   |         |         |         |                                                                |
|                  |  3.2.4 | SecondaryNameNode |   √   |         |         |         |                                                                |
|      hadoop      |        |     DataNode      |        |   √    |   √    |   √    |                                                                |
|                  |        |  ResourceManager  |   √   |         |         |         |                                                                |
|                  |        |    NodeManager    |        |   √    |   √    |   √    |                                                                |
|      spark       | 3.2.3  |   Spark on Yarn   |   √   |   √    |   √    |   √    |          需要编译源码，解决与 hadoop-3.2.4 的兼容问题          |
|      hbase       | 2.4.16 |      HMaster      |   √   |         |         |         |                                                                |
|                  |        |   HRegionServer   |        |   √    |   √    |   √    |                                                                |
|       hive       | 3.1.3  |   Hive on Spark   |   √   |         |         |         |  需要编译源码，解决与 hadoop-3.2.4 和 Spark-3.2.3 的兼容问题   |
|    zookeeper     | 3.6.4  |     Zookeeper     |        |   √    |   √    |   √    |                                                                |
|      kafka       | 3.2.3  |       Kafka       |        |   √    |   √    |   √    |                                                                |
|      mysql       | 8.0.28 |       Mysql       |   √   |         |         |         |                                                                |
|      flume       | 1.11.0 |       Flume       |   √   |   √    |   √    |   √    |               master 消费 Kafka，slaver 采集日志               |
|     maxwell      | 1.29.2 |      Maxwell      |        |   √    |   √    |   √    |                           同步 Mysql                           |
| DolphinScheduler | 3.1.3  |   MasterServer    |        |   √    |         |         |                                                                |
|                  |        |   WorkerServer    |        |         |   √    |   √    |                                                                |
|      datax       | 2022.9 |       DataX       |   √   |         |         |         |            需要替换 Mysql 的驱动 jar，以支持 8.0.x             |
|      presto      |        |    Coordinator    |        |   √    |         |         |                                                                |
|                  |        |      Worker       |        |         |   √    |   √    |                                                                |
|      druid       |        |       Druid       |        |   √    |   √    |   √    |                                                                |
|      kylin       |        |       Kylin       |   √   |         |         |         |                                                                |
|     Superset     |        |     Superset      |   √   |         |         |         |                                                                |
|      atlas       |        |       Atlas       |   √   |         |         |         |                                                                |
|      solr        |        |        solr       |        |   √    |   √    |   √    |                                                                |


### 4.2 组件的安装

  **详见** offline-data-warehouse/doc/0-各组件的安装.md

<br/>

## 5. 数据生成合同步的构、部署和执行 

```bash
    # 拉取项目，并进行构建
    git clone https://github.com/lihuashiyu/offline-data-warehouse.git         # 使用 git 将仓库中的代码和文件克隆到本地 
    cd offline-data-warehouse/ || exit                                         # 进入项目
    ./build.sh                                                                 # 在项目的根目录下，进行构建项目，并将项目上传到服务器
    
    # 登录 master 服务器，然后将部署包上传到 master 服务器用户的 家目录
    cd ~ || exit                                                               # 进入用户家目录
    tar -zxvf ~/offline-data-warehouse.tar.gz                                  # 解压部署包
    
    # 进行集群部署
    cd ~/offline-data-warehouse  || exit                                       # 进入部署路径
    ~/offline-data-warehouse/shell/install.sh                                  # 执行部署脚本，进行多台服务器部署
    
    # 一键启动，将生成的数据库数据、日志数据，同步到 HDFS 的 /hive/tmp/warehouse （注意：此脚本只适用于增量同步）
    ~/offline-data-warehouse/shell/warer-house.sh start                        # 执行部署脚本，进行多台服务器部署
    
    # 查看数据是否同步成功
    ${HADOOP_HOME}/bin/hadoop fs -ls -l /hive/tmp/warehouse/ 
```

# <center>**offline-data-warehouse**</center>

## 1. 项目总体结构

本项目基于 [**B站**](https://www.bilibili.com/) 上 [**尚硅谷离线数仓 5.0**](https://www.bilibili.com/video/BV1AT411j7hu/) 进行学习后的笔记整理，总体的目录结构如下：

```bash
    offline-data-warehouse
     ├── deploy                                            # 各个部署模块
     │     ├── file-kafka                                  # 使用 flume 监控【用户行为日志】，并同步到 kafka
     │     ├── hdfs-mysql                                  # 使用 datax 将 ads 层的数据同步到 mysql
     │     ├── kafka-hdfs                                  # 使用 flume 将 kafka 中的业务数据，同步到 hdfs
     │     ├── mock-db                                     # 模仿【业务数据】生成模块
     │     ├── mock-log                                    # 模仿【用户行为】日志生成模块
     │     ├── mysql-hdfs                                  # 使用 datax 将 MySQL 的全量数据，同步到 hdfs
     │     ├── mysql-kafka                                 # 使用 maxwell 监控 MySQL 增量数据，并同步到 kafka
     │     ├── shell                                       # 项目的部署、组件启停、模块启停脚本
     │     ├── sql                                         # 整个离线数仓使用到的 mysql-sql，hive-sql
     │     └── warehouse                                   # 数仓各层之间的调用脚本
     ├── deploy.sh                                         # 一键打包脚本
     ├── doc                                               # 尚硅谷相关文档
     ├── flume                                             # 自定义 flume 拦截器源码，用于拦截不规则的数据
     └── ReadMe.md                                         # 项目说明文档                                                                
```

<br/>

## 2. 项目架构图

**<center>项目架构图</center>**

![项目架构图](doc/5-%E9%87%87%E9%9B%865.0%E6%9E%B6%E6%9E%84.png)

<br/>

**<center>项目部署图</center>**

![项目部署图](doc/6-%E9%87%87%E9%9B%865.0%E6%9E%B6%E6%9E%84.png)

<br/>

## 3. deploy 打包后的项目模块说明

### 3.1 mock-log 模块

```bash
    # 模拟 生成用户行为日志，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 3.3 章节
    mock-log
     ├── logs                                              # 运行产生的日志目录
     ├── application.yml                                   # mock-log 的配置文件                                       
     ├── cycle.sh                                          # 该脚本调用 mock-log.sh，进行循环生成，默认 10 次
     ├── logback.xml                                       # 日志配置文件
     ├── mock-log.jar                                      # 执行的 jar
     ├── mock-log.sh                                       # mock-log 模块的启停脚本
     └── path.json                                         # 与生成的数据相关 
```

### 3.2 file-kafka 模块

```bash
    # 将生成的 用户行为日志 同步到 kafka，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 4.3 章节
    file-kafka
     ├── file-kafka.conf                                   # flume 监控本地日志文件的配置文件
     ├── file-kafka.sh                                     # 启停脚本
     ├── flume-1.0.jar                                     # 执行 offline-data-warehouse/flume/build.sh 生成的 jar，用于拦截不规则数据
     └── position.json                                     # flume 监控本地文件产生的记录
```

### 3.3 mock-db 模块

```bash
    # 模拟 生成业务数据，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.2 章节
    mock-db
     ├── logs                                              # 运行产生的日志目录
     ├── application.properties                            # mock-db 的配置文件
     ├── cycle.sh                                          # 该脚本调用 mock-db.sh，进行循环生成，默认 10 次
     ├── data.sql                                          # 模拟 mysql 数据库原始的数据
     ├── mock-db.jar                                       # 执行的 jar，修改过尚硅谷原始的 jar，已经支持 mysql 8.0.x
     ├── mock-db.sh                                        # mock-db 模块的启停脚本
     └── table.sql                                         # 电商业务数据 建表语句 
```

### 3.4 mysql-hdfs 模块

```bash
    # 使用 DataX 将生成的 业务数据全量同步到 HDFS，仅在项目部署完成后，初始化的的时候使用一次，后续无需再次操作，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mysql-hdfs
     ├── conf                                              # DataX 将数据库中维度表全量同步到 HDFS 的配置文件，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1 章节
     │     ├── activity_info.json                          # 活动信息表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.1  章节
     │     ├── activity_rule.json                          # 活动规则表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.2  章节
     │     ├── base_category1.json                         # 一级分类表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.6  章节
     │     ├── base_category2.json                         # 二级分类表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.7  章节
     │     ├── base_category3.json                         # 三级分类表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.8  章节
     │     ├── base_dic.json                               # 字典表，          详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.9  章节
     │     ├── base_province.json                          # 省份表，          详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.10 章节
     │     ├── base_region.json                            # 地区表，          详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.11 章节
     │     ├── base_trademark.json                         # 品牌表，          详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.12 章节
     │     ├── cart_info.json                              # 购物车表，        详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.13 章节
     │     ├── coupon_info.json                            # 优惠券信息，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.14 章节 
     │     ├── sku_attr_value.json                         # SKU 平台属性值表，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.27 章节
     │     ├── sku_info.json                               # SKU 信息表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.28 章节
     │     ├── sku_sale_attr_value.json                    # SKU 销售属性表，  详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.29 章节
     │     └── spu_info.json                               # SPU 信息表，      详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.1.30 章节
     ├── GenerateMysqlHdfsJob.py                           # 使用 python 生成 DataX 的 mysql --> hdfs 的配置文件
     └── mysql-hdfs.sh                                     # 该脚本调用 DataX  mysql 数据同步到 hdfs
```

### 3.5 mysql-kafka 模块

```bash
    # 使用 MaxWell 监控 Mysql，用于将产生的 增量业务数据 同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.3 章节
    mysql-kafka 
     ├── config.properties                                 # MaxWell 监控 Mysql 的配置文件
     ├── meta.sql                                          # MaxWell 监控时，在数据库创建的表
     ├── mysql-kafka.sh                                    # 监控数据库启停脚本 
     └── mysql-kafka-init.sh                               # 初始化所有的增量表，只需安装时执行一次
```

### 3.6 kafka-hdfs 模块

```bash
    # 将模拟生成的 用户行为日志 和 增量业务数据，通过 flume 同步到 hdfs，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.3 章节
    kafka-hdfs
     ├── data-db                                           # flume 同步过程中产生的数据存储目录
     ├── check-point                                       # 检查点数据
     │     ├── db                                          # 保存的 增量业务数据 检查点数据 
     │     └── log                                         # 保存的 用户行为日志 检查点数据
     ├── data                                              # flume 同步过程中产生的数据存储目录
     │     ├── db                                          # 增量业务数据  
     │     └── log                                         # 用户行为日志
     ├── flume-1.0.jar                                     # 执行 offline-data-warehouse/flume/build.sh 生成的 jar，用于拦截不规则数据
     ├── kafka-hdfs-db.conf                                # 增量业务数据 同步到 hdfs 的配置文件
     ├── kafka-hdfs-db.sh                                  # 增量业务数据 同步启停脚本
     ├── kafka-hdfs-log.conf                               # 用户行为日志 同步到 hdfs 的配置文件
     └── kafka-hdfs-log.sh                                 # 用户行为日志 同步启停脚本
```

### 3.7 hdfs-mysql 模块

```bash
    # 将 ADS 层数据导出到 Mysql，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 12.2 章节
    hdfs-mysql
     ├── conf                                              # DataX 将 HDFS 全量同步到 Mysql 的配置文件，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11 章节
     │     ├── ads_activity_stats.json                     # 最近30天发布的活动的补贴率，  详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.6.1 章节
     │     ├── ads_coupon_stats.json                       # 最近30天发布的优惠券的补贴率，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.5.1 章节
     │     ├── ads_new_buyer_stats.json                    # 新增交易用户统计，            详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.2.5 章节
     │     ├── ads_order_by_province.json                  # 各省份交易统计，              详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.4.2 章节
     │     ├── ads_page_path.json                          # 路径分析(页面单跳)，          详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.1.2 章节
     │     ├── ads_repeat_purchase_by_tm.json              # 最近7/30日各品牌复购率，      详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.3.1 章节
     │     ├── ads_sku_cart_num_top3_by_cate.json          # 各分类商品购物车存量Top3，    详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.3.4 章节
     │     ├── ads_trade_stats_by_cate.json                # 各品类商品交易统计，          详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.3.3 章节
     │     ├── ads_trade_stats_by_tm.json                  # 各品牌商品交易统计，          详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.3.2 章节
     │     ├── ads_trade_stats.json                        # 交易综合统计，                详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.4.1 章节
     │     ├── ads_traffic_stats_by_channel.json           # 各渠道流量统计，              详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.1.1 章节
     │     ├── ads_user_action.json                        # 用户行为漏斗分析，            详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.2.4 章节
     │     ├── ads_user_change.json                        # 用户变动统计，                详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.2.1 章节
     │     ├── ads_user_retention.json                     # 用户留存率，                  详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.2.2 章节
     │     ├── ads_user_stats.json                         # 用户新增活跃统计，            详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 11.2.3 章节
     ├── GenerateHdfsMysql.py                              # 使用 python 生成 DataX 的 hdfs --> mysql 的配置文件
     └── hdfs-mysql.sh                                     # 该脚本调用 DataX 将 hdfs 数据同步到 mysql
```

### 3.8 sql 模块

```bash
    # 数仓中各层之间的流转，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 7 到 12 章节
    sql
     ├── ads.sql                                           # ADS 建表和插入数据用到的 hive-sql，第 11 章
     ├── dim.sql                                           # DIM 建表和插入数据用到的 hive-sql，第 8  章
     ├── dwd.sql                                           # DWD 建表和插入数据用到的 hive-sql，第 9  章
     ├── dws.sql                                           # DWS 建表和插入数据用到的 hive-sql，第 10 章
     ├── export.sql                                        # ADS 层导出到 mysql 的建表语句    ，第 12 章
     ├── hive.sql                                          # 整个数仓中所有表的建表语句（初始化时使用一次）
     └── ods.sql                                           # ODS 建表和插入数据用到的 hive-sql，第 7  章
```

### 3.9 shell 模块

```bash
    # 部署包的部署、初始化、组件启停，各模块启停脚本
    shell
     ├── component.sh                                      # 各个大数据组件的启停脚本
     ├── data-sync.sh                                      # 将模拟数据同步到 hdfs 的启停脚本
     ├── init.sh                                           # 部署完成后，一键初始化
     ├── range.sh                                          # range 认证脚本：暂时不需要
     ├── warehouse.sh                                      # 数仓中每层之间的计算
     ├── xcall.sh                                          # 在多台服务器执行命令，并查看结果
     └── xync.sh                                           # 文件同步脚本 
```

### 3.10 warehouse 模块

```bash
    # 数仓中各层之间的流转脚本，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 7 到 12 章节
    # *._init.sh 仅在数仓初始化的时候使用，用于同步历史数据
    warehouse
     ├── dwd-dws-init.sh                                   # DWS 层 初始化 数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 10 章节
     ├── dwd-dws.sh                                        # DWS 层  每日  数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 10 章节
     ├── dws-ads.sh                                        # ADS 层  每日  数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的  11.7   章节
     ├── hdfs-ods-init.sh                                  # ODS 层 初始化 数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的   7.1   章节
     ├── hdfs-ods.sh                                       # ODS 层  每日  数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的   7.2   章节
     ├── ods-dim-init.sh                                   # DIM 层 初始化 数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的  8.7.1  章节
     ├── ods-dim.sh                                        # DIM 层  每日  数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的  8.7.2  章节
     ├── ods-dwd_init.sh                                   # DWD 层 初始化 数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 9.20.1  章节
     └── ods-dwd.sh                                        # DWD 层  每日  数据装载，详见文档 offline-data-warehouse/doc/3-电商数据仓库系统.docx 的 9.20.2  章节
```

<br/>

## 4. 基础组件的集群部署

### 4.1 大数据组件及版本说明

**<center>服务器基本信息</center>**

|        |     master      |     slaver1     |     slaver2     |     slaver3     |
|:------:|:---------------:|:---------------:|:---------------:|:---------------:|
|  CPU   |      4C/8T      |      4C/8T      |      4C/8T      |      4C/8T      |
|  内存  |  16GB/3200MHz   |  16GB/3200MHz   |  16GB/3200MHz   |  16GB/3200MHz   |
|  硬盘  |    HDD 40GB     |    HDD 40GB     |    HDD 40GB     |    HDD 40GB     |
|  网卡  |    1000Mbps     |    1000Mbps     |    1000Mbps     |    1000Mbps     |
|   IP   | 192.168.100.100 | 192.168.100.111 | 192.168.100.122 | 192.168.100.133 |
|  系统  | Rocky Linux 9.1 | Rocky Linux 9.1 | Rocky Linux 9.1 | Rocky Linux 9.1 |

<br/>

**<center>组件规划</center>**

|     组件名称     |  版本号  |      子服务       | master | slaver1 | slaver2 | slaver3 |                              说明                              |
|:----------------:|:--------:|:-----------------:|:------:|:-------:|:-------:|:-------:|:--------------------------------------------------------------:|
|       java       | 1.8.321  |       Java        |   √   |   √    |   √    |   √    |                                                                |
|      scala       | 2.12.17  |       Scala       |   √   |   √    |   √    |   √    |                                                                |
|      mysql       |  8.0.28  |       Mysql       |   √   |         |         |         |                                                                |
|                  |          |      NameNode     |   √   |         |         |         |                                                                |
|                  |  3.2.4   | SecondaryNameNode |   √   |         |         |         |                                                                |
|      hadoop      |          |     DataNode      |   √   |   √     |   √    |   √    |                                                                |
|                  |          |  ResourceManager  |   √   |         |         |         |                                                                |
|                  |          |    NodeManager    |        |   √    |   √    |   √    |                                                                |
|      spark       |  3.2.3   |   Spark on Yarn   |   √   |   √    |   √    |   √    |          需要编译源码，解决与 hadoop-3.2.4 的兼容问题          |
|      hbase       |  2.4.16  |      HMaster      |   √   |         |         |         |                                                                |
|                  |          |   HRegionServer   |        |   √    |   √    |   √    |                                                                |
|       hive       |  3.1.3   |   Hive on Spark   |   √   |         |         |         |  需要编译源码，解决与 hadoop-3.2.4 和 Spark-3.2.4 的兼容问题   |
|    zookeeper     |  3.6.4   |     Zookeeper     |        |   √    |   √    |   √    |                                                                |
|      kafka       |  3.2.3   |       Kafka       |        |   √    |   √    |   √    |                                                                |
|      flume       |  1.11.0  |       Flume       |        |   √    |   √    |   √    |               master 消费 Kafka，slaver 采集日志               |
|     maxwell      |  1.29.2  |      Maxwell      |        |   √    |   √    |   √    |                           同步 Mysql                           |
| DolphinScheduler |  3.1.3   |   MasterServer    |   √   |   √    |         |         |                                                                |
|                  |          |   WorkerServer    |        |   √    |   √    |   √    |                                                                |
|      datax       |  2022.9  |       DataX       |   √    |       |         |         |            需要替换 Mysql 的驱动 jar，以支持 8.0.x             |

大数据组件部署请参考：[大数据部署项目](https://github.com/lihuashiyu/bigdata-deploy/)

### 4.2 项目服务规划

<center>项目服务规划</center>

|     服务名称     | 版本号  | master | slaver1 | slaver2 | slaver3 |            说明            |
|:----------------:|:------:|:------:|:-------:|:-------:|:-------:|:--------------------------:|
|     mock-log     |  1.0   |        |   √    |   √    |   √    |         生成用户日志         |
|    file-kafka    |  1.0   |        |   √    |   √    |   √    |         监控生成日志         |
|     mock-db      |  1.0   |        |   √    |   √    |   √    |         生成业务数据         |
|    mysql-hdfs    |  1.0   |   √   |       |         |         |      同步历史数据到 HDFS      |
|    hdfs-mysql    |  1.0   |   √   |       |         |         |     同步 ADS 数据到 Mysql     |
|    mysql-kafka   |  1.0   |        |   √   |         |         |         监控业务数据         |
|  kafka-hdfs-db   |  1.0   |        |        |   √    |         |    将用户日志同步到 HDFS     |
|  kafka-hdfs-log  |  1.0   |        |        |        |    √    |    将业务数据同步到 HDFS     |

<br/>

## 5. 项目构建和使用

```bash
    # 1. 拉取项目，并进行构建
    mkdir -p ~/github                                                          # 创建源码存储路径
    cd ~/github                                                                # 进入源码存储目录
    git clone https://github.com/lihuashiyu/offline-data-warehouse.git         # 使用 git 将仓库中的代码和文件克隆到本地 
    ~/offline-data-warehouse/deploy.sh                                         # 构建项目，并将部署包上传到服务器
    
    # 2. 登录 master 服务器，然后将部署包上传到 master 服务器用户的 家目录
    cd ~ || exit                                                               # 进入用户家目录
    tar -zxvf ~/offline-data-warehouse-1.0.tar.gz                              # 解压部署包
    
    # 3. 进行集群部署以及初始化
    cd ~/offline-data-warehouse  || exit                                       # 进入部署路径
    bash ~/offline-data-warehouse/shell/init.sh                                # 执行初始化脚本，进行多台服务器部署完后初始化
    
    # 4. 一键启动，将模拟生成的 业务数据、用户行为日志，同步到 HDFS 的 /warehouse/tmp/ （注意：此脚本只适用于增量同步）
    ~/offline-data-warehouse/shell/data-sync.sh start                          # 执行部署脚本，进行多台服务器部署
    
    # 5. 查看数据是否同步成功
    ${HADOOP_HOME}/bin/hadoop fs -ls -l /warehouse/tmp/
```

<br/>

## 6. HDFS 路径说明

```bash
    /warehouse
      ├── ads                                              # ADS 层表数据存储路径
      ├── conf                                             # 配置文件   存储路径
      ├── data                                             # 第三方文件 存储路径
      ├── db                                               # 同步的 业务数据 路径
      ├── dim                                              # DIM 层表数据存储路径
      ├── dwd                                              # DWD 层表数据存储路径
      ├── dws                                              # DWS 层表数据存储路径
      ├── jars                                             # 自定义 udf、udaf、udtf 函数 jar
      ├── log                                              # 同步的 行为日志 路径
      ├── ods                                              # ODS 层表数据存储路径
      └── tmp                                              # TMP 临时数据存储路径
```

<br/>

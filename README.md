
# <center>offline-data-warehouse</center>

## 1. 项目总体结构

本项目基于 [**B站**](https://www.bilibili.com/) 上 [**尚硅谷离线数仓 5.0**](https://www.bilibili.com/) 进行学习后的笔记整理，总体的目录结构如下：

```shell
    offline-data-warehouse
      ├─doc                                                                      # 使用到的文档说明
      ├─deploy                                                                   # 各个部署模块
            ├─file-kafka                                                         # 使用 flume 监控【用户行为日志】，并同步到 kafka
            ├─hdfs-mysql                                                         # 使用 datax 将 ads 层的数据同步到 mysql
            ├─kafka-hdfs                                                         # 使用 flume 将 kafka 中的数据，同步到 hdfs
            ├─mock-db                                                            # 模仿【用户行为】日志生成模块
            ├─mock-log                                                           # 模仿【业务数据】生成模块
            ├─mysql-hdfs                                                         # 使用 datax 将 MySQL 的全量数据，同步到 hdfs 
            └─mysql-kafka                                                        # 使用 maxwell 监控 MySQL 增量数据，并同步到 kafka
      ├─flume                                                                    # 自定义 flume 拦截器源码，用于拦截不规则的数据
      ├─python                                                                   # 生成 datax 的配置文件
            ├─GenerateConfig.py                                                  # 生成 datax 的配置文件代码
            ├─mysql-hdfs                                                         # datax 的 mysql 到 hdfs 同步配置文件
            └─hdfs-mysql                                                         # datax 的 hdfs 到 mysql 同步配置文件
      ├─shell                                                                    # 相关部署、启停等脚本
            ├─component.sh                                                       # 各大数据组件启停脚本
            ├─install.sh                                                         # 一键部署脚本，用于部署使用
            ├─install.sh                                                         # 一键部署脚本，用于部署使用
            └─warer-house.sh                                                     # 项目集群的启停脚本
      ├─sql                                                                      # 整个离线数仓使用到的 mysql-sql，hive-sql，doris-sql
            ├─ads.sql                                                            # 数仓中的 ADS 层用到的 hive-sql
            ├─diros.sql                                                          # 数仓中，ADS 导出到 doris 用到的 sql
            ├─dws.sql                                                            # 数仓中的 DWS 层用到的 hive-sql
            └─ods.sql                                                            # 数仓中的 ODS 层用到的 hive-sql
      ├─ReadMe                                                                   # 项目说明文档
```


## 2. 项目架构图



## 3. 项目模块说明

### 3.1 mock-log 模块

```shell
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 3.3 章节
    mock-log
       ├─application.yml                                                       # mock-log 的配置文件
       ├─cycle.sh                                                              # 该脚本调用 mock-log.sh，进行循环生成，默认 10 次
       ├─mock-log.jar                                                          # 执行的 jar
       ├─mock-log.sh                                                           # mock-log 模块的启停脚本
       ├─logback.xml                                                           # 日志配置文件
       └─path.json                                                             # 与生成的数据相关
```

### 3.2 file-kafka 模块

```shell
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/1-用户行为采集平台.docx 的 4.3 章节
    file-kafka
       ├─data                                                                  # 执行过程中产生的数据
            ├─                                                                 #
       ├─file-kafka.properties                                                 # 配置文件
       ├─file-kafka.sh                                                         # 启停脚本
       └─flume-1.0.jar                                                         # flume 模块源码编译后的 jar，用于拦截不规则数据
```

### 3.3 mock-db 模块

```shell
    # 模拟生成用户行为日志，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 2.2 章节
    mock-db
       ├─application.properties                                                # mock-db 的配置文件
       ├─cycle.sh                                                              # 该脚本调用 mock-log.sh，进行循环生成，默认 10 次
       ├─data.sql                                                              # 模拟 mysql 数据库原来的数据
       ├─mock-db.jar                                                           # 执行的 jar，修改过尚硅谷原始的 jar，已经支持 mysql 8.0.x
       ├─mock-db.sh                                                            # mock-db 模块的启停脚本
       └─table.sql                                                             # 建表语句
```

### 3.4 mysql-hdfs 模块

```shell
    # 使用 DataX 将表中的数据全量同步到 HDFS，仅在项目部署的时候使用一次，后续无需再次操作，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mock-db
       ├─*.json                                                                # 各个表的同步配置文件
       └─mysql-hdfs.sh                                                         # 该脚本调用 DataX 进行数据同步
```


### 3.5 mysql-kafka 模块

```shell
    # 用于监控 Mysql，当 mock-db 在 Mysql 中产生增量数据时，将增量数据同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mock-db
       ├─mysql-kafka.sh                                                        # MaxWell 监控 Mysql 的启停脚本
       └─mysql-hdfs.sh                                                         # 该脚本调用 DataX 进行数据同步
```

### 3.6 kafka-hdfs 模块

```shell
    # 用于监控 Mysql，当 mock-db 在 Mysql 中产生增量数据时，将增量数据同步到 kafka，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mock-db
       ├─mysql-kafka.sh                                                        # MaxWell 监控 Mysql 的启停脚本
       └─mysql-hdfs.sh                                                         # 该脚本调用 DataX 进行数据同步
```

### 3.7 hdfs-mysql 模块

```shell
    # 使用 DataX 将 Hive ADS 层的表数据同步到 mysql，详见文档 offline-data-warehouse/doc/2-业务数据采集平台.docx 的 3.2 章节
    mock-db
       ├─*.json                                                                # 各个表的同步配置文件
       └─hdfs-mysql.sh                                                         # 该脚本调用 DataX 进行数据同步
```


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

  **详见** offline-data-warehouse/doc/0-各组件的安装



## 5. 数据生成合同步的构、部署和执行 

```shell
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

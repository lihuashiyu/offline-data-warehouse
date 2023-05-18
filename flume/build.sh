#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  build.sh
#    CreateTime    ：  2023-02-23 23:09
#    Author        ：  lihua shiyu 
#    Email         ：  lihuashiyu@github.com
#    Description   ：  build.sh 被用于 ==> flume 源码编译为 jar
# =========================================================================================

SERVICE_DIR=$(cd $(dirname "$0") || exit; pwd)             # 需要执行的服务路径
ROOT_DIR=$(cd "${SERVICE_DIR}/../" || exit; pwd)           # 项目根路径
MAVEN_HOME="/opt/apache/maven"                             # Maven 安装目录
# LOG_FILE="flume-build-$(date +%F-%H-%M-%S).log"          # 操作日志存储
LOG_FILE="flume-build-$(date +%F).log"                     # 操作日志存储

# 1. 创建日志目录 logs
mkdir -p "${ROOT_DIR}/logs/" 

# 2. 切换到源码路径
cd "${SERVICE_DIR}" || exit

# 3. 使用 Maven 进行构建
echo "============================ flume 源码构建 ==========================="
"${MAVEN_HOME}/bin/mvn" clean package -DskipTests=true >> "${ROOT_DIR}/logs/${LOG_FILE}" 2>&1

# 4. 复制生成的 jar 到 deploy/file-kafka
echo "============================== 复制 jar =============================="
cp -fp "${SERVICE_DIR}"/target/*with*.jar "${ROOT_DIR}"/deploy/file-kafka/flume-1.0.jar
cp -fp "${SERVICE_DIR}"/target/*with*.jar "${ROOT_DIR}"/deploy/kafka-hdfs/flume-1.0.jar

# 5. 清理退出 
echo "============================== 清理退出 =============================="
"${MAVEN_HOME}/bin/mvn" clean >> "${ROOT_DIR}/logs/${LOG_FILE}" 2>&1 
exit 0

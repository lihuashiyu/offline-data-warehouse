#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  deploy.sh
#    CreateTime    ：  2023-02-23 23:07
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  deploy.sh 被用于 ==> 对整体项目的构建，打包
# =========================================================================================

ROOT_DIR=$(cd $(dirname "$0") || exit; pwd)               # 项目根路径
ALIAS_NAME="data-warehouse-1.0"                            # 项目别名
LOG_FILE="deploy-$(date +%F-%H-%M-%S).log"                 # 操作日志存储
PACKAGE_FILE=data-warehouse-1.0.tar.gz                     # 生成的部署包名


# 1. 创建日志目录
mkdir -p "${ROOT_DIR}/logs/"

# 2. 构建 flume 源码包
echo "============================ flume 源码构建 ============================"
"${ROOT_DIR}/flume/build.sh" >> "${ROOT_DIR}/logs/${LOG_FILE}" 2>&1 
sleep 1

# 3.打包
echo "============================= 部署包制作中 ============================="
rm  "${ROOT_DIR}/deploy/${PACKAGE_FILE}"
mv  "${ROOT_DIR}/deploy/" "${ROOT_DIR}/${ALIAS_NAME}"
cd  "${ROOT_DIR}" || exit 
tar -zcvf "${PACKAGE_FILE}" "${ALIAS_NAME}/" >> "${ROOT_DIR}/logs/${LOG_FILE}" 2>&1 
sleep 1

# 4. 清理退出
echo "============================== 移动部署包 =============================="
cd  "${ROOT_DIR}" || exit 
mv  "${ROOT_DIR}/${PACKAGE_FILE}"  "${ROOT_DIR}/${ALIAS_NAME}/"
mv  "${ROOT_DIR}/${ALIAS_NAME}"    "${ROOT_DIR}/deploy/"

# 5. 退出
echo "=============================== 完成退出 ==============================="
exit 0

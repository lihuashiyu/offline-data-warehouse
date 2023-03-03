#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  install.sh
#    CreateTime    ：  2023-02-24 01:42
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  install.sh 被用于 ==> 一键部署脚本
# =========================================================================================
    
    
PROJECT_DIR=$(cd "$(dirname "$0")/../" || exit; pwd)           # 项目根路径
HOST_LIST=(master slaver1 slaver2 slaver3)
USER=$(whoami)
TARGET_PATH=$(cd -P "${PROJECT_DIR}" || exit; pwd)/


cd "${PROJECT_DIR}/" || exit
module_list=$(ls -d *)

for module in ${module_list}
do
    echo "mkdir -p ${PROJECT_DIR}/${module}/logs"
done
# 
# 
# for host_name in "${HOST_LIST[@]}"
# do
#     echo "============================== 向主机（${host_name}）同步数据 =============================="
#     # 3. 执行同步
#     rsync -zav --delete  "${TARGET_PATH}"  "${USER}@${host_name}:${TARGET_PATH}"
# done

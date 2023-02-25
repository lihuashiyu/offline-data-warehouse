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

# 1. 遍历循环读取主机 ${HOST_LIST[@]}
for host_name in "${HOST_LIST[@]}"
do
    echo "============================== 向主机（${host_name}）同步数据 =============================="    
    # 2. 创建目录
    ssh "${USER}@${host_name}" "mkdir -p ${PROJECT_DIR}; exit "
    
    # 3. 执行同步
    rsync -zav --delete  "${PROJECT_DIR}"  "${USER}@${host_name}:${PROJECT_DIR}"
done

#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  xcall.sh
#    CreateTime    ：  2023-02-24 02:27
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  xcall.sh 被用于 ==> 在集群中查看服务器执行，命令后的返回状况
# =========================================================================================


HOST_LIST=(master slaver1 slaver2 slaver3)                 # 集群主机
USER=$(whoami)                                             # 获取当前登录用户

# 1. 判断输入参数：若参数大于 1，给出提示并推出；若参数为空，则查看文件夹内容，否则执行命令
if [ "$#" -lt 1 ]; then
    cmd="pwd; ls -l" 
elif [ "$#" -gt 1 ] || [ "${1}" = "" ]; then
	echo "    脚本最多可输入一个参数 ......   "
	exit
else 
    cmd="$*"
fi

printf "\n================================================================================\n"

# 2. 遍历所有的主机，执行命令
for host_name in "${HOST_LIST[@]}"
do
    echo "    ********** 在主机 ${host_name} 上执行命令：${cmd} **********    "
    # ssh "${USER}@${host_name}" "$@"
    
    # 3. 执行命令
    ssh "${USER}@${host_name}" "source ~/.bashrc; source ~/.bash_profile; ${cmd}"
done

printf "================================================================================\n\n"

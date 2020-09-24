#!/bin/bash
## 需求：循环复制文件到集群所有节点的相同节点的相同目录下
## 使用方式：脚本+需要复制的文件名称
### 脚本编写大致步骤
#1、获取脚本传入参数，参数个数,如果传入的参数为0个，退出
paramNum=$#

if((paramNum==0));
then
  echo no args;
exit;
fi
#2、获取文件名称
p1=$1
## basename /opt/lagou/servers/hadoop  返回的是hadoop，最后的文件
file_name=`basename $p1`
echo f_name=${file_name}
#3、获取文件的绝对路径，获取到文件的目录信息
## ## dirname /hadoop  返回的是.，最后的文件的所在的绝对路径
dir_name=`cd -P $(dirname $p1);pwd`
## dirname /opt/lagou/servers/hadoop  返回的是/opt/lagou/servers，最后的文件的所在的绝对路径
#dir_name=`dirname $p1`
echo dirname=${dir_name}
#4、获取到当前用户信息
user=`whoami`
#5、执行rsync命令，循化执行,要把数据发送到集群中所有的节点中
for((host=121;host<124;host++));
do
echo -------target hostname=linux${host}--------
rsync -rvl ${dir_name}/${file_name} $user@linuxd:${dir_name}
done
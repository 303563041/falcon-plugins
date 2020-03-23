#!/bin/bash
source /etc/profile

DESTINATION="http://127.0.0.1:1999/v1/push"
ts=`date +%s`
hostname=`hostname`
java_pids=`ps -elf | grep "java" | grep -v grep |awk '{print $4}'`
java_version=`/data/home/user00/usr/jdk/bin/java -version 2>&1 | grep version | awk '{print $NF}' | tr -d '"'`
jstat_command="/data/home/user00/usr/jdk/bin/jstat"

function CURL() {
  curl -X POST -d "[{\"metric\": \"$1\", \"endpoint\": \"$2\", \"timestamp\": $3,\"step\": 60,\"value\": $4,\"counterType\": \"$5\",\"tags\": \"$6\"}]" $DESTINATION
}

function get_gc_stats() {
    appname=`jps | grep $1 | awk '{print $2}'`
    if [ $? == 0 ];then
        threads_num=`pstree -p $1 | wc -l`
        gc_stat=`$jstat_command -gcutil $1 | tail -1`
        s0_utilization_rate=`echo $gc_stat | awk '{print $1}'`
        s1_utilization_rate=`echo $gc_stat | awk '{print $2}'`
        eden_utilization_rate=`echo $gc_stat | awk '{print $3}'`
        old_utilization_rate=`echo $gc_stat | awk '{print $4}'`

        if [[ $java_version =~ ^1\.8 ]];then
          permanent_utilization_rate=0
        else
          permanent_utilization_rate=`echo $gc_stat | awk '{print $5}'`
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          metaspace_utilization_rate=`echo $gc_stat | awk '{print $5}'`
        else
          metaspace_utilization_rate=0
        fi
        
        if [[ $java_version =~ ^1\.8 ]];then
          compressed_class_utilization_rate=`echo $gc_stat | awk '{print $6}'`
        else
          compressed_class_utilization_rate=0
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          young_gc_number=`echo $gc_stat | awk '{print $7}'`
        else
          young_gc_number=`echo $gc_stat | awk '{print $6}'`
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          young_gc_time=`echo $gc_stat | awk '{print $8}'`
        else
          young_gc_time=`echo $gc_stat | awk '{print $7}'`
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          full_gc_number=`echo $gc_stat | awk '{print $9}'` 
        else
          full_gc_number=`echo $gc_stat | awk '{print $8}'`
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          full_gc_time=`echo $gc_stat | awk '{print $10}'` 
        else
          full_gc_time=`echo $gc_stat | awk '{print $9}'`
        fi

        if [[ $java_version =~ ^1\.8 ]];then
          total_gc_time=`echo $gc_stat | awk '{print $11}'` 
        else
          total_gc_time=`echo $gc_stat | awk '{print $10}'`
        fi

        CURL threads_num $hostname $ts $threads_num "GAUGE" port=
        CURL s0_utilization_rate $hostname $ts $s0_utilization_rate "GAUGE" appname=$appname
        CURL s1_utilization_rate $hostname $ts $s1_utilization_rate "GAUGE" appname=$appname
        CURL eden_utilization_rate $hostname $ts $eden_utilization_rate "GAUGE" appname=$appname
        CURL old_utilization_rate $hostname $ts $old_utilization_rate "GAUGE" appname=$appname
        CURL permanent_utilization_rate $hostname $ts $permanent_utilization_rate "GAUGE" appname=$appname
        CURL metaspace_utilization_rate $hostname $ts $metaspace_utilization_rate "GAUGE" appname=$appname
        CURL compressed_class_utilization_rate $hostname $ts $compressed_class_utilization_rate "GAUGE" appname=$appname
        CURL young_gc_number $hostname $ts $young_gc_number "GAUGE" appname=$appname
        CURL young_gc_time $hostname $ts $young_gc_time "GAUGE" appname=$appname
        CURL full_gc_number $hostname $ts $full_gc_number "GAUGE" appname=$appname
        CURL full_gc_time $hostname $ts $full_gc_time "GAUGE" appname=$appname
        CURL total_gc_time $hostname $ts $total_gc_time "GAUGE" appname=$appname
    fi
}

for pid in $java_pids
do
    get_gc_stats $pid 
done
#!/usr/bin/env bash
# Licensed to CMRI
# 
#

# DGS-Q
export DGSQ_HOME=/home/hadoop/dgsq

# Java Enviroments Varaiables
export JAVA_HOME=

# Use cloudera CDH, no need to set Hadoop/Spark env
# Hadoop Enviroments Varaiables
#export HADOOP_HOME=$DGSQ_HOME/hadoop-2.7.6
#export HADOOP_INSTALL=$HADOOP_HOME
#export HADOOP_MAPRED_HOME=$HADOOP_HOME
#export HADOOP_COMMON_HOME=$HADOOP_HOME
#export HADOOP_HDFS_HOME=$HADOOP_HOME
#export YARN_HOME=$HADOOP_HOME
#export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native

# Spark Enviroments Varaiables
#export SPARK_HOME=$DGSQ_HOME/spark-2.3.1-bin-hadoop2.7
#export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
#export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Path
#export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
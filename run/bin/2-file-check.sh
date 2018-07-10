#!/usr/bin/env bash


# Import env
source ../etc/dgsq-env.sh

# Start file check.

usage="Usage: file-check.sh []"

# get arguments

echo "Starting file-check"
$SPARK_HOME/bin/spark-submit \
  --class "com.cmcc.cmri.dgsq.FileCheck" \
  --master local[4] \
  /home/wujia/Work/IdeaProjects/dgsq/target/dgsq-1.0.jar /home/wujia/Downloads/all_2017g2.csv

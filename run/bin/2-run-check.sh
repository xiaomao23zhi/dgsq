#!/usr/bin/env bash
###############################################################################
# DGS-Q backend shell scripts
#
# CMRI@2018
###############################################################################

function print_log() {
    DATE=`date "+%Y-%m-%d %H:%M:%S"`
    USER=$(whoami)
    echo "${DATE} ${USER} [INFO] $0 $@"
}

function usage() {
   echo "Usage: rule-check.sh [xdr interface] [xdr file]"
}

###############################################################################
# Main
###############################################################################
# get arguments
if [ $# != 2 ]
then
    usage
    exit 1

else
    XDR_FILE=$1
    XDR_SCHEMA=$2
fi

XDR_DATE=`echo ${XDR_FILE} | awk -F '_' '{print $2}' | cut -c 1-8`
XDR_FILE_FULL=${XDR_HDFS_HOME}/${XDR_DATE}/${XDR_FILE}

print_log "Starting 2-run-check for $XDR_INTERFACE hdfs://${HDFS_HOST}/${HDFS_HOME}/${XDR_DATE}/${XDR_FILE}"
spark2-submit --class cmcc.cmri.dgsq.run.RunCheck \
    --master yarn \
    --deploy-mode client \
    --driver-memory 6g \
    --executor-memory ${SPARK_EXE_MEMORY} \
    --executor-cores ${SPARK_EXE_CORES} \
    --num-executors ${SPARK_NUM_EXECUTOR} \
    --queue default \
    ${DGSQ_HOME}/jar/dgsq-core-1.0-jar-with-dependencies.jar \
    hdfs://${HDFS_HOST}/${HDFS_HOME}/${XDR_DATE}/${XDR_FILE} ${XDR_SCHEMA}
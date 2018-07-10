#!/usr/bin/env bash
###############################################################################
#
#
#
###############################################################################
function print_log() {
    DATE=`date "+%Y-%m-%d %H:%M:%S"` 
    USER=$(whoami)
    echo "${DATE} ${USER} [INFO] $0 $@"
}

function usage() {
   echo "Usage: rule-check.sh [xdr interface] [xdr file]"
}

# get arguments
if [ $# != 2 ]
then
    usage
    exit 1
else
    XDR_INTERFACE=$1
    XDR_FILENAME=$2
fi

print_log "Starting rule-check for $XDR_INTERFACE $XDR_FILENAME"
$SPARK_HOME/bin/spark-submit \
  --class "com.cmcc.cmri.dgsq.run.RuleCheck" \
  --master local[4] \
  /home/wujia/Work/IdeaProjects/dgsq/target/dgsq-1.0.jar LTE /home/wujia/Downloads/all_2017g2.csv

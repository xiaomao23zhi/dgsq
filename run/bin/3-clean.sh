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
   echo "Usage: 3-clean.sh"
}

###############################################################################
# Main
###############################################################################
# Step 1. Clean HDFS
print_log "Cleaning HDFS: hdfs dfs -rm -r ${HDFS_HOME}/${CLEAN_HDFS_DATE}"
if [ `echo ${CLEAN_HDFS_DATE} | awk '{print length($0)}'` -eq 8 ]
then
    hdfs dfs -rm -r ${HDFS_HOME}/${CLEAN_HDFS_DATE}
fi

# Step 2. Clean mongo
sql="db.dropDatabase()"
print_log "Cleaning Mongo: ${MONGO_HOME}/mongo $MONGO_HOST:$MONGO_PORT/${CLEAN_MONGO_DATE} --eval \"$sql\""
if [ `echo ${CLEAN_MONGO_DATE} | awk '{print length($0)}'` -eq 9 ]
then
    ${MONGO_HOME}/mongo $MONGO_HOST:$MONGO_PORT/${CLEAN_MONGO_DATE} --eval "$sql"
fi

# Step 3. Clean ${DGSQ_HOME}/log
print_log "Cleaning log: find  ${DGSQ_HOME}/log -name \"*${CLEAN_LOG_DATE}*.log\" | xargs rm -f"
if [ `echo ${CLEAN_LOG_DATE} | awk '{print length($0)}'` -eq 8 ]
then

    find  ${DGSQ_HOME}/log -name "*${CLEAN_LOG_DATE}*.log" | xargs rm -f
fi
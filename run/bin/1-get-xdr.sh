#!/usr/bin/env bash
###############################################################################
# DGS-Q backend shell scripts
#
# CMRI@2018
###############################################################################

function usage() {
   echo "Usage: get-xdr.sh [xdr file name] [xdr schema]"
}

# 输出日志
function print_log() {
    DATE=`date "+%Y-%m-%d %H:%M:%S"`
    USER=$(whoami)
    echo "${DATE} ${USER} [INFO] $0 $@"
}

# 过程调用
function invoke_func() {
    print_log "Invoke function [$1]"
    . $1
    print_log "Exit function [$1]"
}

# 写入Mongodb
function mongo_log() {
    time_stamp=`date "+%Y-%m-%d %H:%M:%S"`
    sql="db.q_logs.insert( { program_name:\"$1\", run_status:\"$2\", timestamp:\"${time_stamp}\", print_message:\"$3\" } )"
    print_log "$sql"
    mongo $MONGO_HOST:$MONGO_PORT/$MONGO_DB --eval "$sql"
}

###############################################################################
# Main
###############################################################################
if [ $# != 1 ]
then
    usage
    exit 1
else
    XDR_FILE=$1
fi

XDR_DATE=`echo ${XDR_FILE} | awk -F '_' '{print $2}' | cut -c 1-8`
XDR_FILE_FULL=${XDR_HDFS_HOME}/${XDR_DATE}/${XDR_FILE}

# create HDFS dir
print_log "hdfs dfs -mkdir -p hdfs://${HDFS_HOST}${HDFS_HOME}/${XDR_DATE}"
hdfs dfs -mkdir -p hdfs://${HDFS_HOST}${HDFS_HOME}/${XDR_DATE}

# Step 1. peek it xdr file exist
while :
do
    file_list=`ftp -inv ${DPI_HOST} << EOF
user ${DPI_USER} ${DPI_PASS}
cd ${DPI_PATH}
dir ${XDR_FILE}
quit
EOF
`
    ret=$(echo $file_list | grep -c ${XDR_FILE})
    if [ $ret -eq  0 ]
    then
        print_log "XDR file [${XDR_FILE_FULL}] not found."
        sleep 10
    else
        print_log "XDR file [${XDR_FILE_FULL}] OK."
        break
    fi
done

# Step 2. get XDR file from ftp
print_log "hadoop distcp -overwrite ftp://${DPI_USER}:${DPI_PASS}@${DPI_HOST}/${DPI_PATH}/${XDR_FILE} hdfs://${HDFS_HOST}/${HDFS_HOME}/${XDR_DATE}"
hadoop distcp -overwrite ftp://${DPI_USER}:${DPI_PASS}@${DPI_HOST}/${DPI_PATH}/${XDR_FILE} hdfs://${HDFS_HOST}/${HDFS_HOME}/${XDR_DATE}

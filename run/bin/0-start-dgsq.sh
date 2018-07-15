#!/usr/bin/env bash

###############################################################################
# DGS-Q backend shell scripts
# 
# CMRI@2018
###############################################################################

# Import env & config
source ../etc/dgsq-env.sh
source ../etc/dgsq.conf

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

function usage() {
   echo "Usage: dgsq-start.sh [args]"
} 

###############################################################################
# Main
###############################################################################
print_log "Starting..."
mongo_log $0 $STATUS_RUN "Starting ..."
xdr_date=`date "+%Y%m%d%H0000"`

# for each interface
for xdr_i in `echo "$INTERFACE_LIST" | sed 's/,/\n/g'`
do
    xdr_f=${xdr_i}_${xdr_date}_${XDR_VENDER_ID}_${XDR_DEVICE_ID}_${XDR_FILE_SEQUENCE}.txt
    print_log "getting XDR file: [${xdr_f}]"

    # Step 1. Check conf

    # Step 2. ftp XDR file to local
    call_func "$DGSQ_HOME/bin/get-xdr.sh xdr_i xdr_f"

    # Step 3. run quality rules on XDR file
    call_func "$DGSQ_HOME/bin/rule-check.sh xdr_i xdr_f"

    # Step 3. clean
done

write_to_mongo $0 $STATUS_SUC "SUCCESS"

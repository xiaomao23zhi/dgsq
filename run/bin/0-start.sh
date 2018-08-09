#!/usr/bin/env bash
###############################################################################
# DGS-Q backend shell scripts
#  Usage: 5 * * * * sh /home/cmcc/run/bin/0-start.sh >> /home/cmcc/run/log/0-start.`date +"\%Y\%m\%d"`.log 2>&1
# CMRI@2018
###############################################################################

# Import env & config
source /home/cmcc/run/etc/dgsq-env.sh
source /home/cmcc/run/etc/dgsq.conf

function usage() {
   echo "Usage: 0-dgsq-start.sh [args]"
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
print_log "Starting 0-start.sh..."
#mongo_log $0 $STATUS_RUN "Starting ..."

print_log "XDR_FREQ = ${XDR_FREQ}"

if [ ${XDR_FREQ} == "HH" ]
then
    xdr_date=`date "+%Y%m%d%H0000"`
else
    xdr_date=`date "+%Y%m%d%H%M00"`
fi

run_date=`date "+%Y%m%d"`

# for each interface
for xdr_interface in `echo "$XDR_INTERFACE" | sed 's/,/\n/g'`
do
    xdr_f=${xdr_interface}_${xdr_date}_${XDR_VENDER_ID}_${XDR_DEVICE_ID}_${XDR_FILE_SEQUENCE}.txt
    xdr_s=XDR_SCHEMA_${xdr_interface}
    print_log "getting XDR file: [${xdr_f}] [${!xdr_s}]"

    # Step 1. Check conf

    # Step 2. ftp XDR file to local
    print_log "Invoke: ${DGSQ_HOME}/bin/1-get-xdr.sh ${xdr_f}"
    . ${DGSQ_HOME}/bin/1-get-xdr.sh ${xdr_f} >> ${DGSQ_HOME}/log/1-get-xdr.${run_date}.log
    print_log "Finished: "

    # Step 3. run quality rules on XDR file
    print_log "Invoke: ${DGSQ_HOME}/bin/2-run-check.sh ${xdr_f} ${!xdr_s}}"
    . ${DGSQ_HOME}/bin/2-run-check.sh ${xdr_f} ${!xdr_s}} >> ${DGSQ_HOME}/log/2-run-check.${run_date}.log
    print_log "Finished: "
done

# Step 3. clean
print_log "Invoke:  ${DGSQ_HOME}/bin/3-clean.sh"
. ${DGSQ_HOME}/bin/3-clean.sh >> ${DGSQ_HOME}/log/3-clean.${run_date}.log

#write_to_mongo $0 $STATUS_SUC "SUCCESS"

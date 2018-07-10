#!/usr/bin/env bash

###############################################################################
#
#
#
###############################################################################

function usage() {
   echo "Usage: foo.sh"
}

function write_to_mongo() {
    sql="db.q_logs.insert( { program_name:\"$1\", run_status:\"$2\", timestamp:\"${time_stamp}\", print_message:\"$3\" } )" 
    echo $sql
    mongo 127.0.0.1:27017/test --eval "$sql"
}

# JavaScript for mongodb command
MONGO_SCRIPT=./.mongo.$$.js

touch ${MONGO_SCRIPT}

if [ -f ${MONGO_SCRIPT} ]
then
    rm -f ${MONGO_SCRIPT}
fi

time_stamp=`date "+%Y-%m-%d %H:%M:%S"`
#write_to_mongo "db.q_logs.insert( { programe:\"foo.sh\", timestamp:\"${time_stamp}\", status:0, message:\"SUCCESS!\" } )"
write_to_mongo $0 0 "TEST!"

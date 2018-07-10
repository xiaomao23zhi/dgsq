#!/usr/bin/env bash

###############################################################################
#
#
#
###############################################################################

function usage() {
   echo "Usage: get-xdr.sh [xdr interface] [xdr file]"
}

if [ $# != 2 ]
then
    usage
    exit 1
else
    XDR_INTERFACE=$1
    XDR_FILENAME=$2
fi



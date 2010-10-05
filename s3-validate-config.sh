#!/bin/bash
#
# siznax 2010

MAX_ITEM_SIZE_GB=10

usage="s3-validate-config.sh config s3cfg"
CONFIG=$1
S3CFG=$2

if [ -z $CONFIG ] || [ ! -f $CONFIG ] || [ -z $S3CFG ] || [ ! -f $S3CFG ]
then
    echo "Usage: $usage"
    exit 1
fi

job_dir=`grep ^job_dir $CONFIG | awk '{print $2}'`
xfer_dir=`grep ^xfer_dir $CONFIG | awk '{print $2}'`

DRAINME="$job_dir/DRAINME"

sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
max_size=`grep ^max_size $CONFIG | awk '{print $2}'`
warc_naming=`grep ^WARC_naming $CONFIG | awk '{print $2}'`
block_delay=`grep ^block_delay $CONFIG | awk '{print $2}'`
max_block_count=`grep ^max_block_count $CONFIG | awk '{print $2}'`
retry_delay=`grep ^retry_delay $CONFIG | awk '{print $2}'`

description=`grep ^description $CONFIG | awk '{print $2}'`
operator=`grep ^operator $CONFIG | awk '{print $2}'`
collections=`grep ^collection_ $CONFIG | awk '{print $2}' | tr "\n" ' '`
title_prefix=`grep ^title_prefix $CONFIG | cut -d \" -f 2`
creator=`grep ^creator $CONFIG | cut -d \" -f 2`
sponsor=`grep ^sponsor $CONFIG | cut -d \" -f 2`
scancenter=`grep ^scanningcenter $CONFIG | awk '{print $2}'`

# get IAS3 keys
if [ -f $S3CFG ]
then
    access_key=`grep access_key $S3CFG | awk '{print $3}'`
    secret_key=`grep secret_key $S3CFG | awk '{print $3}'`
else 
    echo "ERROR: S3CFG file not found: $S3CFG"
	exit 1
fi

# check config
if [ ! -d $job_dir ]
then
    echo "ERROR: invalid job_dir: $job_dir"
    exit 1
elif [ ! -d $xfer_dir ]
then
    echo "ERROR: invalid xfer_dir: $xfer_dir"
    exit 1
elif [ -z $sleep_time ]
then
    echo "ERROR: null sleep_time."
    exit 1
elif [ -z $max_size ] || [ $max_size -lt 1 ] || [ $max_size -gt $MAX_ITEM_SIZE_GB ] 
then
    echo "ERROR: invalid max_size: $max_size"
    exit 1
elif [ -z $warc_naming ] || [ $warc_naming -gt 2 ]
then
    echo "ERROR: invalid warc_naming: $warc_naming"
    exit 1
elif [ -z $block_delay ] || [ ! `echo $block_delay | grep [[:digit:]]` ]
then
    echo "ERROR: invalid block_delay: $block_delay"
    exit 1
elif [ -z $retry_delay ] || [ ! `echo $retry_delay | grep [[:digit:]]` ]
then
    echo "ERROR: invalid retry_delay: $retry_delay"
    exit 1
elif [ -z $max_block_count ] || [ ! `echo $max_block_count | grep [[:digit:]]` ]
then
    echo "ERROR: invalid max_block_count: $max_block_count"
    exit 1
elif [ -z $description ] || [ `echo $description | grep '{describe_effort}'` ]
then
    echo "ERROR: invalid description: $description"
    exit 1
elif [ -z $operator ] || [ $operator == "tbd@archive.org" ]
then
    echo "ERROR: invalid crawl operator: $operator"
    exit 1
elif [ -z "$collections" ] || [ `echo $collections | grep -c 'TBD'` -ne 0 ]
then
    echo "ERROR: invalid collection(s): $collections"
    exit 1
elif [ -z "$title_prefix" ] || [ "$title_prefix" == 'TBD Crawldata' ]
then
    echo "ERROR: invalid title_prefix: $title_prefix"
    exit 1
elif [ -z "$creator" ] || [ -z "$sponsor" ] || [ -z $scancenter ]
then
    echo "ERROR: invalid creator, sponsor, or scanningcenter"
    exit 1
elif [ ! `echo $access_key | grep [[:alnum:]]` ]
then
    echo "ERROR: invalid access_key: $access_key"
    exit 1
elif [ ! `echo $secret_key | grep [[:alnum:]]` ]
then
    echo "ERROR: invalid secret_key: $secret_key"
    exit 1
else
    echo "config OK!"
    echo "  job_dir          = $job_dir"
    echo "  xfer_dir         = $xfer_dir"
    echo "  DRAINME          = $DRAINME"
    echo "  max_size         = $max_size"
    echo "  sleep_time       = $sleep_time"
    echo "  block_delay      = $block_delay"
    echo "  max_block_count  = $max_block_count"
    echo "  retry_delay      = $retry_delay"
    echo "  collection(s)    = $collections"
    echo "  title_prefix     = $title_prefix"
    echo "  creator          = $creator"
    echo "  sponsor          = $sponsor"
    echo "  scanningcenter   = $scancenter"
    echo "  operator         = $operator"
    echo "  access_key       = $access_key"
    echo "  secret_key       = $secret_key"
    exit 0
fi

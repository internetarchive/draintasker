#!/bin/bash
#
# siznax 2010

MAX_ITEM_SIZE_GB=10

CONFIG=$1
S3CFG=$2

if [ -z $CONFIG ] || [ ! -f $CONFIG ]
then
    echo "ERROR: config file not found: $CONFIG"
    exit 1
fi

if  [ -z $S3CFG ] || [ ! -f $S3CFG ]
then
    echo "ERROR: s3cfg file not found: $S3CFG"
    exit 1
fi

crawljob=`grep ^crawljob $CONFIG | awk '{print $2}'`
job_dir=`grep ^job_dir $CONFIG | awk '{print $2}'`
xfer_dir=`grep ^xfer_dir $CONFIG | awk '{print $2}'`

sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
max_size=`grep ^max_size $CONFIG | awk '{print $2}'`
warc_naming=`grep ^WARC_naming $CONFIG | awk '{print $2}'`
block_delay=`grep ^block_delay $CONFIG | awk '{print $2}'`
max_block_count=`grep ^max_block_count $CONFIG | awk '{print $2}'`
retry_delay=`grep ^retry_delay $CONFIG | awk '{print $2}'`

description=`grep ^description $CONFIG | cut -d \" -f 2`
operator=`grep ^operator $CONFIG | awk '{print $2}'`
collections=`grep ^collections $CONFIG | awk '{print $2}' | tr '/' ' '`
title_prefix=`grep ^title_prefix $CONFIG | cut -d \" -f 2`
creator=`grep ^creator $CONFIG | cut -d \" -f 2`
sponsor=`grep ^sponsor $CONFIG | cut -d \" -f 2`
contributor=`grep ^contributor $CONFIG | cut -d \" -f 2`
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

# check keywords
for key in CRAWLHOST CRAWLJOB START_DATE END_DATE
do
    found=`echo "$description" | grep $key`
    if [ $? != 0 ]
    then
        echo "ERROR: description missing keyword: $key => $description"
        exit 1
    fi
done

# check config
if [ -z $crawljob ]
then
    echo "ERROR: invalid crawljob: $crawljob"
    exit 1
elif [ ! -d $job_dir ]
then
    echo "ERROR: missing job_dir: $job_dir"
    exit 1
elif [ ! -d $xfer_dir ]
then
    echo "ERROR: missing xfer_dir: $xfer_dir"
    exit 1
elif [ -z $sleep_time ]
then
    echo "ERROR: null sleep_time."
    exit 1
elif [ -z $max_size ] || [ $max_size -lt 1 ] ||\
     [ $max_size -gt $MAX_ITEM_SIZE_GB ] 
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
elif [ -z $max_block_count ] ||\
     [ ! `echo $max_block_count | grep [[:digit:]]` ]
then
    echo "ERROR: invalid max_block_count: $max_block_count"
    exit 1
elif [ -z "$description" ] ||\
     [ `echo "$description" | grep '{describe_effort}'` ]
then
    echo "ERROR: invalid description: $description"
    exit 1
elif [ -z $operator ] || [ $operator == "tbd@archive.org" ]
then
    echo "ERROR: invalid crawl operator: $operator"
    exit 1
elif [ -z "$collections" ] ||\
     [ `echo $collections | grep -c 'TBD'` -ne 0 ]
then
    echo "ERROR: invalid collection(s): $collections"
    exit 1
elif [ -z "$title_prefix" ] || [ "$title_prefix" == 'TBD Crawldata' ]
then
    echo "ERROR: invalid title_prefix: $title_prefix"
    exit 1
elif [ -z "$creator" ]
then
    echo "ERROR: invalid creator: $creator"
    exit 1
elif [ -z "$sponsor" ] 
then
    echo "ERROR: invalid sponsor: $sponsor"
    exit 1
elif [ -z "$contributor" ] 
then
    echo "ERROR: invalid contributor: $contributor"
    exit 1
elif [ -z $scancenter ]
then
    echo "ERROR: invalid scanningcenter: $scancenter"
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
    echo "  crawljob         = $crawljob"
    echo "  job_dir          = $job_dir"
    echo "  xfer_dir         = $xfer_dir"

    echo "  sleep_time       = $sleep_time"
    echo "  max_size         = $max_size"
    echo "  warc_naming      = $warc_naming"
    echo "  block_delay      = $block_delay"
    echo "  max_block_count  = $max_block_count"
    echo "  retry_delay      = $retry_delay"

    echo "  description      = $description"
    echo "  operator         = $operator"
    echo "  collection(s)    = $collections"
    echo "  title_prefix     = $title_prefix"
    echo "  creator          = $creator"
    echo "  sponsor          = $sponsor"
    echo "  contributor      = $contributor"
    echo "  scanningcenter   = $scancenter"

    echo "  access_key       = $access_key"
    echo "  secret_key       = $secret_key"

    exit 0
fi

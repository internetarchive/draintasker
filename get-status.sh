#!/bin/bash
# get-status crawldata_dir xfer_dir disk
# 
# report 
#   crawled_warcs   num w/arcs in crawldata_dir
#   packed_warcs    num w/arcs packed into warc_series
#   verified_warcs  num w/arcs transferred and verified
#   warc_series     num warc_series
#   n PACKED        num PACKED files and last w/arc series PACKED path
#   n MANIFEST      num MANIFEST files and last w/arc series MANIFEST path
#   n TASK          num TASK files and last w/arc series TASK path
#   n SUCCESS       num SUCCESS files and last w/arc series SUCCESS path
#   n TOMBSTONE     num TOMBSTONE files and last w/arc series TOMBSTONE path
#   .open files extant
#   ERROR files extant
#   disk usage
# 
# siznax 2009
if [ "$3" ]
then

  echo "JOB:" `echo $1 | tr '/' ' ' | awk '{print $NF}'`
  echo "host:" `hostname`
  echo "crawldata:" $1
  echo "xfer_dir:" $2

  if [ ! -e $1 ]
  then 
    echo "ERROR: directory not found: $1"
    exit 2
  fi
  if [ ! -e $2 ]

  then 
    echo "ERROR: directory not found: $2"
    exit 2
  fi

  crawled_warcs=`ls -l $1/*.{arc,warc}.gz | wc -l`
  echo "crawled warcs: $crawled_warcs"

  packed_warcs=`find $2 \( -name \*.warc.gz -o -name \*.arc.gz \) | wc -l`
  echo "packed warcs: $packed_warcs"

  verified_warcs=`find $2 -name "*.tombstone" | wc -l`
  echo "verified warcs: $verified_warcs"

  num_series=`find $2 -type d | wc -l`
  (( num_series-- ))
  echo "warc series: $num_series"

  for FILE in PACKED MANIFEST TASK SUCCESS TOMBSTONE
  do
    files=`find $2 -name "$FILE"`
    num_files=`echo $files | tr " " "\n" | wc -l`
    last_file=`echo $files | tr " " "\n" | tail -1`
    echo "$num_files $last_file"
  done

  open=`find $2 -name "*.open"`
  num_open=`echo $open | tr " " "\n" | wc -l`
  if [ -n "$open" ]
  then
    echo "found ($num_open) open files: "
    echo $open | tr " " "\n"
  fi

  errors=`find $2 -name "ERROR"`
  num_errors=`echo $errors | tr " " "\n" | wc -l`
  if [ -n "$errors" ]
  then
    echo "found ($num_errors) ERROR files: "
    echo $errors | tr " " "\n"
  fi

  echo "disk usage: $3"
  df -h $3

else
  echo "$0 crawldata_dir xfer_dir disk"
fi

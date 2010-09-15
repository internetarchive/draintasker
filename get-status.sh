#!/bin/bash
# get-status.sh crawldata_dir xfer_dir disk
# 
# reports
# 
#   job, host, disk, crawldata_dir, xfer_dir, drainme files
#   dtmon procs     PID of currently running dtmon.sh scripts
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
#   RETRY files extant
# 
# siznax 2009

if [ "$3" ]
then

  crawldata=$1
  transfer=$2
  disk=$3

  job=`echo $1 | tr '/' ' ' | awk '{print $3}'`
  host=`hostname`
  disk_usage=`df -h ${disk} | tail -1 | tr -s ' '`

  echo "====" $job $host $disk "===="
  echo "job:" $job 
  echo "host:" $host
  echo "disk:" $disk $disk_usage 

  # crawldata dir
  if [ ! -e ${crawldata} ]
  then 
    echo "ERROR: directory not found: ${crawldata}"
    exit 2
  else
    echo "crawldata:" $crawldata
  fi

  # transfer dir 
  if [ ! -e ${transfer} ]
  then 
    echo "ERROR: directory not found: ${transfer}"
    exit 2
  else
    echo "transfer:" $transfer
  fi

  drainme="${crawldata}/DRAINME"
  if [ ! -e $drainme ]
  then
    drainme="not found"
  fi

  finish_drain="${crawldata}/FINISH_DRAIN"
  if [ ! -e $finish_drain ]
  then
    finish_drain="not found"
  fi

  echo "drainme:" $drainme
  echo "finish_drain:" $finish_drain

  dtmon=`pgrep -l dtmon | tr "\n" " "`
  echo "dtmon_procs: $dtmon"

  crawled_warcs=`ls -l ${crawldata}/*.{arc,warc}.gz 2> /dev/null | wc -l`
  echo "crawled_w/arcs: $crawled_warcs"

  packed_warcs=`find ${transfer} \( -name \*.warc.gz -o -name \*.arc.gz \) | wc -l`
  echo "packed_w/arcs: $packed_warcs"

  tombstones=`find ${transfer} -name "*.tombstone" | wc -l`
  echo "tombstones: $tombstones"

  num_series=`find ${transfer} -type d | wc -l`
  (( num_series-- ))
  echo "warc_series: $num_series"

  for FILE in PACKED MANIFEST TASK SUCCESS TOMBSTONE RETRY
  do
    files=`find ${transfer} -name "$FILE" | sort`
    num_files=`echo $files | tr " " "\n" | wc -l`
    last_file=`echo $files | tr " " "\n" | tail -1`
    if [ -n "$last_file" ]
    then
      echo "  $num_files $last_file"
    fi
  done

  extras=('*.open' 'RETRY*' 'ERROR' )
  kinds=(OPEN RETRY ERROR)

  for (( i=0; i < ${#extras[@]}; i++ ))
  do
      found=`find ${transfer} -name "${extras[$i]}" | sort`
      num_found=`echo $found | tr " " "\n" | wc -l`
      if [ -n "$found" ]
      then
        echo "found ($num_found) ${kinds[$i]} files: "
        for f in `echo $found`; do echo "  $f"; done
      fi
      unset found
  done

else
  echo "$0 crawldata_dir xfer_dir disk"
fi

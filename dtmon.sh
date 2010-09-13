#!/bin/bash
# draintasker monitor - dtmon.sh job_dir xfer_dir 
#
# while DRAINME file exists, run drain-job.sh then sleep for sleep_time
#
# run like this: 
#
#   $ screen 
#   $ ./dtmon.sh job_dir xfer_dir thumper | tee -a log_file
#
#  DRAINME     {job_dir}/DRAINME
#  job_dir     /{crawldata}/{job_name}
#  xfer_dir    /{rsync_dir}/{job_name}
#  thumper     destination storage node
#  sleep_time  seconds to sleep between checks for DRAINME file
#  dtmon.cfg   configuration params in this file
#
# siznax 2009

usage="$0 job_dir xfer_dir"

CONFIG="./dtmon.cfg"

if [ -n "$2" ]
then

  job_dir=$1
  xfer_dir=$2
  DRAINME="$job_dir/DRAINME"

  echo $0 `date`

  while [ -e $DRAINME ]
  do

    # parse config
    sleep_time=`grep ^sleep_time $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
    thumper=`grep ^thumper       $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
    
    # check config
    if [ -z $sleep_time ]
    then
        echo "ERROR: null sleep_time, aborting."
        exit 1
    elif [ -z $thumper ]
    then
        echo "ERROR: null thumper, aborting."
        exit 1
    elif [ $thumper == 'TARGET_THUMPER' ]
    then
        echo "ERROR: config needs $thumper"
        exit 1
    else
        echo "config OK"
        echo "  sleep_time = $sleep_time"
        echo "  thumper    = $thumper"
    fi

    echo "drain-job.sh $job_dir $xfer_dir"
    ./drain-job.sh $job_dir $xfer_dir $thumper

    echo "sleeping $sleep_time seconds at" `date`
    sleep $sleep_time

  done

  echo "DRAINME file not found: $DRAINME"
  exit 1

else
  echo $usage
  exit 1
fi

echo $0 "Done." `date`
#!/bin/bash
# draintasker monitor - dtmon.sh job_dir xfer_job_dir thumper
#
# while DRAINME file exists, run drain-job.sh then sleep for sleep_time
#
# run like this: 
#
#   $ screen 
#   $ ./dtmon.sh job_dir xfer_job_dir thumper | tee -a log_file
#
#  DRAINME       {job_dir}/DRAINME
#  job_dir       /{crawldata}/{job_name}
#  xfer_job_dir  /{rsync_path}/{job_name}
#  thumper       destination storage node
#  sleep_time    seconds to sleep between checks for DRAINME file
#  dtmon.cfg     configuration params in this file
#
# siznax 2009

usage="$0 job_dir xfer_job_dir thumper"

sleep_time=`grep sleep_time dtmon.cfg | cut -d ' ' -f 2`

if [ -n "$3" ]
then

  job_dir=$1
  xfer_job_dir=$2
  thumper=$3
  DRAINME="$job_dir/DRAINME"

  echo $0 `date`

  while [ -e $DRAINME ]
  do

    echo "drain-job.sh $job_dir $xfer_job_dir $thumper"
    ./drain-job.sh $job_dir $xfer_job_dir $thumper

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
#!/bin/bash
#
# s3 draintasker monitor - s3-dtmon.sh job_dir xfer_job_dir 
#
# while DRAINME file exists, run drain-job.sh then sleep for sleep_time
#
# run like this: 
#
#   $ screen 
#   $ ./dtmon.sh job_dir xfer_job_dir | tee -a log_file
#
#  DRAINME       {job_dir}/DRAINME
#  job_dir       /{crawldata}/{job_name}
#  xfer_job_dir  /{rsync_path}/{job_name}
#  sleep_time    seconds to sleep between checks for DRAINME file
#  dtmon.cfg     configuration params in this file
#  ~/.s3cfg      http://archive.org/help/abouts3.txt
#
# siznax 2010

usage="$0 job_dir xfer_job_dir"

if [ -n "$2" ]
then

  CONFIG="./dtmon.cfg"
  
  job_dir=$1
  xfer_job_dir=$2
  DRAINME="$job_dir/DRAINME"

  echo $0 `date`

  while [ -e $DRAINME ]
  do

    # parse config
    sleep_time=`grep ^sleep_time    $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
    access_key=`grep ^s3_access_key $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
    secret_key=`grep ^s3_secret_key $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
    
    # check config
    if [ -z $sleep_time ]
    then
        echo "ERROR: null sleep_time, aborting."
        exit 1
    elif [ $access_key == 'YOUR_ACCESS_KEY' ]
    then
        echo "ERROR: config needs $access_key"
        exit 1
    elif [ $secret_key == 'YOUR_SECRET_KEY' ]
    then
        echo "ERROR: config needs $secret_key"
        exit 1
    else
        echo "config OK"
        echo "  sleep_time = $sleep_time"
        echo "  access_key = $access_key"
        echo "  secret_key = $secret_key"
    fi

    echo "s3-drain-job.sh $job_dir $xfer_job_dir"
    ./s3-drain-job.sh $job_dir $xfer_job_dir

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
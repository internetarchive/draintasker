#!/bin/bash
#
# while DRAINME file exists, run drain-job.sh then sleep for sleep_time
#
# dtmon.sh config
#
#   config  config params in this file, see dtmon.cfg
#
# run like this: 
#
#   $ screen 
#   $ /home/user/dt/dtmon.sh config | tee -a log_file
#
# PREREQUISITES
#
#   DRAINME  {job_dir}/DRAINME
#
# DEPENDENCIES
#
#   ~/.ias3cfg  put your keys here, see abouts3
#
# CONFIGURATION PARAMS
#
#   job_dir     /{crawldata}/{job_name}
#   xfer_dir    /{rsync_path}/{job_name}
#   max_size    max size in GB of warcs to be transferred
#   sleep_time  seconds to sleep between checks for DRAINME file
#
# PREREQUISITES
#
#   DRAINME  {job_dir}/DRAINME
#
# SEE ALSO
#
#   http://archive.org/help/abouts3.txt
#
# siznax 2010

usage="config"

if [ -n "$1" ]
then

  CONFIG=$1
  S3CFG="$HOME/.ias3cfg"

  if [ ! -f $CONFIG ]
  then
      echo "ERROR: config file not found: $CONFIG"
      exit 1
  elif [ ${CONFIG:0:1} != '/' ]
  then
      echo "ERROR: must give fullpath for config: $CONFIG"
      exit 1
  else
      job_dir=`grep ^job_dir $CONFIG | awk '{print $2}'`
      DRAINME="$job_dir/DRAINME"
      sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
  fi

  echo `basename $0` `date`

  while [ 1 ]
  do

    if [ -e $DRAINME ]
    then

        ./s3-validate-config.sh $CONFIG $S3CFG
        
        if [ $? == 0 ]
        then
            xfer_dir=`grep ^xfer_dir $CONFIG | awk '{print $2}'`
            max_size=`grep ^max_size $CONFIG | awk '{print $2}'`
            sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
            warc_naming=`grep ^WARC_naming $CONFIG | awk '{print $2}'`
        else
            echo "ERROR: invalid config: $CONFIG"
            exit 1
        fi
        
        # echo "Aborting."
        # exit 99
        
        echo "s3-drain-job.sh $job_dir $xfer_dir $max_size $warc_naming"
        ./s3-drain-job.sh $job_dir $xfer_dir $max_size $warc_naming $CONFIG

    else

        echo "DRAINME file not found: $DRAINME"

    fi

    sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
    echo "sleeping $sleep_time seconds at" `date`
    sleep $sleep_time

  done

else
  echo "Usage:" `basename $0` $usage
  exit 1
fi

echo `basename $0` "done." `date`
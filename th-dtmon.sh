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
#   $ ./dtmon.sh config | tee -a log_file
#
# PREREQUISITES
#
#   DRAINME  {job_dir}/DRAINME
#
# CONFIGURATION PARAMS
#
#   job_dir     /{0,1,2,3}/crawling/{crawljob}/warcs
#   xfer_dir    /{0,1,2,3}/incoming/{crawljob}
#   sleep_time  seconds to sleep between checks for DRAINME file
#   thumper     destination storage node
#
# siznax 2009

echo "ERROR: sorry, the thumpers are full, exiting."
exit 99

MAX_ITEM_SIZE_GB=10

usage="$0 config"

if [ -n "$1" ]
then

  CONFIG=$1

  if [ -f $CONFIG ]
  then
      job_dir=`grep ^job_dir $CONFIG | awk '{print $2}'`
      DRAINME="$job_dir/DRAINME"
      sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
  else
      echo "ERROR: config file not found: $CONFIG"
      exit 1
  fi

  echo $0 `date`

  while [ 1 ]
  do

    if [ -e $DRAINME ]
    then

        # update config
        xfer_dir=`grep ^xfer_dir $CONFIG | awk '{print $2}'`
        thumper=`grep ^thumper $CONFIG | awk '{print $2}'`
        max_size=`grep ^max_size $CONFIG | awk '{print $2}'`
        warc_naming=`grep ^WARC_naming $CONFIG | awk '{print $2}'`
        sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
        
        # check config
        if [ ! -d $xfer_dir ]
        then
            echo "ERROR: invalid xfer_dir: $xfer_dir"
            exit 1
        elif [ -z $max_size ] || [ $max_size -lt 1 ] || [ $max_size -gt $MAX_ITEM_SIZE_GB ] 
        then
            echo "ERROR: invalid max_size: $max_size"
            exit 1
        elif [ -z $sleep_time ]
        then
            echo "ERROR: null sleep_time, aborting."
            exit 1
        elif [ -z $thumper ] || [ $thumper == 'TBD' ]
        then
            echo "ERROR: invalid thumper: $thumper"
            exit 1
        else
            echo "config OK!"
            echo "  job_dir     = $job_dir"
            echo "  xfer_dir    = $xfer_dir"
            echo "  max_size    = $max_size"
            echo "  sleep_time  = $sleep_time"
            echo "  thumper     = $thumper"
            echo "  warc_naming = $warc_naming"
            echo "  DRAINME     = $DRAINME"
        fi
        
        # echo "Aborting."
        # exit 99

        echo "drain-job.sh $job_dir $xfer_dir $thumper $max_size $warc_naming"
        ./drain-job.sh $job_dir $xfer_dir $thumper $max_size $warc_naming

    else

        echo "DRAINME file not found: $DRAINME"

    fi

    sleep_time=`grep ^sleep_time $CONFIG | awk '{print $2}'`
    echo "sleeping $sleep_time seconds at" `date`
    sleep $sleep_time

  done

else
  echo $usage
  exit 1
fi

echo $0 "Done." `date`
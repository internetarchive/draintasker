#!/bin/bash
#
# s3-drain-job-s3.sh config
#
# run draintasker processes on a crawl job in single mode.
# the idea here is to keep a crawl draining while not spending
# too much time on any one process. if there is a backlog of
# prerequisites, then each task can be run in a separate 
# process in non-single mode to catch up.
#
# SEE ALSO
#
#  pack-warcs.sh
#  make-manifests.sh
#  s3-launch-transfers.sh
#  delete-warcs.sh
#
# siznax 2010

usage="config"

if [ -f "$1" ]
then

  CONFIG=$1
  job_dir=`./config.py $CONFIG job_dir`
  xfer_job_dir=`./config.py $CONFIG xfer_dir`
  max_size=`./config.py $CONFIG max_size`
  warc_naming=`./config.py $CONFIG WARC_naming`
  compactify=`./config.py $CONFIG compact_names`

  # DEBUG
  echo "  CONFIG       $CONFIG      "
  echo "  job_dir      $job_dir     "
  echo "  xfer_job_dir $xfer_job_dir"
  echo "  max_size     $max_size    "
  echo "  warc_naming  $warc_naming "
  echo "  compactify   $compactify  "
  exit 99

  echo `basename $0` `date`

  if [ -e $job_dir ] 
  then

    # check for xfer_job_dir
    if [ ! -e $xfer_job_dir ]
    then 
      echo "ERROR: xfer_job_dir not found: $xfer_job_dir" 
    fi

    # pack a single series
    ./pack-warcs.sh $job_dir $xfer_job_dir $max_size\
        $warc_naming 1 single $compactify
    if [ $? != 0 ]
    then
      echo "ERROR packing warcs: $?"
      exit 1
    fi

    # make a single manifest
    ./make-manifests.sh $xfer_job_dir single
    if [ $? != 0 ]
    then
      echo "ERROR making manifests: $?"
      exit 1
    fi

    # launch a single task
    ./s3-launch-transfers.sh $CONFIG 1 single
    if [ $? != 0 ]
    then
      echo "ERROR launching transfers: $?"
      exit 1
    fi

    # delete verified warcs
    ./delete-verified-warcs.sh $xfer_job_dir 1
    if [ $? != 0 ]
    then
      echo "ERROR deleting warcs: $?"
      exit 1
    fi

  else
    echo "ERROR: job_dir not found: $job_dir"
    exit 1
  fi
else
  echo "Usage:" `basename $0` $usage
  exit 1
fi
echo `basename $0` "done." `date`

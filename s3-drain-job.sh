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

PG=$0; test -h $PG && PG=$(readlink $PG)
BIN=$(dirname $PG)

usage="config"

if [ -z "$1" ]; then
  echo Usage: $(basename $0) $usage
  exit 1
fi
CONFIG=$1
if [ ! -f $CONFIG ]; then
  echo config file not found: $CONFIG
  exit 1
fi

job_dir=`$BIN/config.py $CONFIG job_dir`
xfer_job_dir=`$BIN/config.py $CONFIG xfer_dir`
max_size=`$BIN/config.py $CONFIG max_size`
warc_naming=`$BIN/config.py $CONFIG WARC_naming`
compactify=`$BIN/config.py $CONFIG compact_names`

# DEBUG
# echo "  CONFIG       $CONFIG      "
# echo "  job_dir      $job_dir     "
# echo "  xfer_job_dir $xfer_job_dir"
# echo "  max_size     $max_size    "
# echo "  warc_naming  $warc_naming "
# echo "  compactify   $compactify  "
# exit 99

echo $(basename $0) $(date)

# bunch of prerequisites
if [ ! -e $job_dir ]; then
  echo "ERROR: job_dir not found: $job_dir"
  exit 1
fi
if [ ! -e $xfer_job_dir ]; then
  echo "ERROR: xfer_job_dir not found: $xfer_job_dir"
  exit 1
fi

# pack a single series
$BIN/pack-warcs.sh $job_dir $xfer_job_dir $max_size\
    $warc_naming 1 single $compactify || {
  echo "ERROR packing warcs: $?"
  exit 1
}
# make a single manifest
$BIN/make-manifests.sh $xfer_job_dir single || {
  echo "ERROR making manifests: $?"
  exit 1
}

# launch a single task
$BIN/s3-launch-transfers.sh $CONFIG 1 single || {
  echo "ERROR launching transfers: $?"
  exit 1
}

# delete verified warcs
$BIN/delete-verified-warcs.sh $xfer_job_dir 1 || {
  echo "ERROR deleting warcs: $?"
  exit 1
}

echo $(basename $0) "done." $(date)

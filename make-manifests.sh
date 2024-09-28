#!/bin/bash 
#
# make-manifests.sh config xfer_job_dir [mode]
#
# foreach PACKED file in xfer_job_dir, computes md5sum of 
# W/ARCs into MANIFEST file if not extant
#
#  config        config params in this file
#  xfer_job_dir  /{rsync_path}/{JOB}
#  JOB           crawl job name
#  PACKED        /{rsync_path}/{JOB}/{warc_series}/PACKED
#  warc_series   group of warcs limited by size or count (e.g. 10G)
#  MANIFEST      /{rsync_path}/{JOB}/{warc_series}/MANIFEST
#  [mode]        single = make manifest for 1 series, then exit
#
# siznax 2009

PG=$0; test -h $PG && PG=$(readlink $PG)
BIN=$(dirname $PG)
: ${GETCONF:=$BIN/config.py}

usage="config xfer_job_dir [mode]"

function report_done {
    echo "$warc_count warcs $manifest_count manifests"
    echo $(basename $0) done. $(date)
}

packed_count=0
warc_count=0
manifest_count=0

if [ -z "$1" ]; then
  echo Usage: $(basename $0) $usage
  exit 1
fi

mode=${3:-0}

CONFIG=$1
if [ -n "$CONFIG" ]; then
  if [ ! -f $CONFIG ]; then
    echo "ERROR: config not found: $CONFIG"
    exit 1
  fi
# validate configuration
  $GETCONF $CONFIG || {
    echo "ERROR: invalid config: $CONFIG"
    exit 1
  }
  MD5SUM=$($GETCONF $CONFIG md5sum)
fi


echo $(basename $0) $(date)

for d in $(find $2 -mindepth 1 -maxdepth 1 -type d | sort); do

  PACKED="$d/PACKED"
  [ -e $PACKED ] || continue

  (( packed_count++ ))

  OPEN="$d/MANIFEST.open"
  MANIFEST="$d/MANIFEST"

  if [ -e "$MANIFEST" ]; then
    #echo "MANIFEST exists: $MANIFEST"
    continue
  fi
  if [ -e "$OPEN" ]; then
    echo "OPEN file exists: $OPEN"
    continue
  fi

  echo $d:
  (
    # MANIFEST column 2 should have no directory
    cd $d
    flock -n -s 100 || {
      echo "ERROR could not lock $OPEN"
      exit 1
    }
    for f in $(ls *.{arc,warc,arc.gz,warc.gz} 2>/dev/null); do
      (( warc_count++ ))
      if [ $MD5SUM -eq '0' ]; then
        echo "  NO md5sum $(basename $f) >> $OPEN"
        echo "-  $(basename $f)" >&100
      else
        echo "  md5sum $(basename $f) >> $OPEN"
        md5sum $f >&100 || {
          echo "ERROR: md5sum failed with status: $?"
          exit 2
        }
      fi
    done
  ) 100>$OPEN
  echo "  mv $(basename $OPEN) $(basename $MANIFEST)"
  mv $OPEN $MANIFEST || {
    echo "ERROR: mv failed"
    exit 1
  }

  (( manifest_count++ ))
  # check mode
  if [ $mode == 'single' ]; then
    echo "mode = $mode, exiting normally."
    break
  fi
done

report_done

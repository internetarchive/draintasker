#!/bin/bash 
#
# make-manifests.sh xfer_job_dir [mode]
#
# foreach PACKED file in xfer_job_dir, computes md5sum of 
# W/ARCs into MANIFEST file if not extant
#
#  xfer_job_dir  /{rsync_path}/{JOB}
#  JOB           crawl job name
#  PACKED        /{rsync_path}/{JOB}/{warc_series}/PACKED
#  warc_series   group of warcs limited by size or count (e.g. 10G)
#  MANIFEST      /{rsync_path}/{JOB}/{warc_series}/MANIFEST
#  [mode]        single = make manifest for 1 series, then exit
#
# siznax 2009

usage="xfer_job_dir [mode]"

TARGET_FILES='*.{arc,warc,arc.gz,warc.gz}'
MD5SUM=md5sum

function doitem {
  local d="$1" MANIFEST="$d/MAIFEST" OPEN="$MANIFEST.open"
  [ -e "$d/PACKED" ] || {
      (( notpacked_count++ ))
      return 1
  }
  [ -e "$MANIFEST" ] && return 1
  set -o noclobber
  echo -n '' > $OPEN || return 1
  echo $d
  for f in $(cd $d; ls $TARGET_FILES 2>/dev/null); do
      (( warc_count++ ))
      echo " md5sum $(basename $f) >> $OPEN"
      (cd $d; $MD5SUM $f) >>$OPEN || {
	  echo "ERROR: $MD5SUM failed with status: $?"
	  exit 2
      }
  done
  mv $OPEN $MANIFEST || {
      echo "ERROR: mv $OPEN $MANIFEST failed"
      exit 1
  }
}
function report_done {
    echo "$warc_count warcs $manifest_count manifests"
    echo $(basename $0) done. $(date)
}

notpacked_count=0
packed_count=0
warc_count=0
manifest_count=0

if [ -z "$1" ]; then
  echo Usage: $(basename $0) $usage
  exit 1
fi
mode=${2:-0}

echo $(basename $0) $(date)

for d in $(find $1 -mindepth 1 -maxdepth 1 -type d | sort); do

  if doitem $d; then
      (( manifest_count++ ))
      if [ $mode == single ]; then
	  echo "mode = $mode, exiting normally."
	  break
      fi
  fi
done

report_done

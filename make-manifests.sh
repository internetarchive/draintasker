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

function report_done {
    echo "$warc_count warcs $manifest_count manifests"
    echo `basename $0` "done." `date`
}

packed_count=0
warc_count=0
manifest_count=0

if [ -n "$1" ]
then

  if [ -n "$2" ]
  then
    mode=$2
  else
    mode=0
  fi

  echo `basename $0` `date`

  back=`pwd`
  cd $1

  for d in `find $1 -type d | sort`; do

    PACKED="$d/PACKED"

    if [ -e "$PACKED" ]; then

      (( packed_count++ ))

      cd $d
      open="$d/MANIFEST.open"
      MANIFEST="$d/MANIFEST"
      warc_series=`echo $d | egrep -o '/([^/]*)$' | tr -d "/"`

      if [ -e "$MANIFEST" ]; then
        echo "MANIFEST exists: $MANIFEST"
        continue
      fi

      if [ -e "$open" ]; then
        echo "OPEN file exists: $open"
        continue
      fi

      echo "==== $warc_series/MANIFEST ===="
      echo "OPEN    :  $open"
      echo "MANIFEST:  $MANIFEST"
      
      touch $open
      if [ $? != 0 ]; then
        echo "ERROR could not touch file:" $open
        exit 1
      fi

      for f in `ls *.{arc,warc}.gz 2>/dev/null`; do

        (( warc_count++ ))

        echo "  md5sum $f >> OPEN"
        md5sum $f >> $open
        if [ $? != 0 ]
        then
            echo "ERROR: md5sum failed with status: $?"
            exit 2
        fi
        
      done

      echo "mv OPEN MANIFEST"
      mv $open $MANIFEST
      if [ $? != 0 ]; then
        echo "ERROR! mv failed"
        exit 1
      fi

      (( manifest_count++ ))

      # check mode
      if [ $mode == 'single' ]
      then
        echo "mode = $mode, exiting normally."
	report_done
        exit 0
      fi

    fi

    open=''
    MANIFEST=''

  done

  cd $back

else
  echo "Usage:" `basename $0` $usage
  exit 1  
fi

report_done

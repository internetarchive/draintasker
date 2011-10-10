#!/bin/bash
# delete-verified-warcs.sh xfer_job_dir [force]
#
# for each warc_series, if TOMBSTONE found, find each 
# original_warc in warc_series, and if matching warc_tombstone 
# found and tombstone_count and manifest_count agree, then 
# delete original_warc.
#
#  xfer_job_dir    /{path}/{job_name}
#  warc_series     {xfer_job_dir}/{warc_series}
#  TOMBSTONE       {warc_series}/TOMBSTONE
#  original_warc   {warc_series}/*.w/arc.gz
#  warc_tombstone  {warc_series}/*.w/arc.gz.tombstone
#  [force]         1 = do not query user
#
# siznax 2009

usage="xfer_job_dir [force]"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

if [ -n "$1" ]
then

  echo `basename $0` `date`

  xfer_job_dir=$1
  total_rm_count=0

  # loop over warc_series
  for d in `find $1 -mindepth 1 -maxdepth 1 -type d`
  do

    warc_series=`echo $d | tr '/' ' ' | awk '{print $NF}'`
    TOMBSTONE="$d/TOMBSTONE"
    MANIFEST="$d/MANIFEST"
  
    # count original warcs in series
    orig_warcs=`find $d \( -name \*.warc.gz -o -name \*.arc.gz \)`
    if [ -n "$orig_warcs" ]
    then
      warc_count=`echo $orig_warcs | tr " " "\n" | wc -l`
    else
      warc_count=0
    fi

    # check for TOMBSTONE
    if [ -e $TOMBSTONE ]
    then
  
      echo "TOMBSTONE exists: $TOMBSTONE"
  
      tombstone_count=`wc -l $TOMBSTONE | awk '{print $1}'`
      manifest_count=`wc -l $MANIFEST | awk '{print $1}'`
  
      # check manifest, tombstone count
      if [ ! $tombstone_count -eq $manifest_count ]
      then
        echo "ERROR: count mis-match:"\
             "($tombstone_count) tombstone"\
             "($manifest_count) manifest"\
             "in series: $warc_series"
        exit 2
      fi
  
      if [ $warc_count == 0 ]
      then 
        continue
      else
        echo "found ($warc_count) warcs in: $d"
      fi
  
      echo "  tombstone_count: $tombstone_count"
      echo "  manifest_count: $manifest_count"
      echo "  warc_count: $warc_count"
  
      # remove original warcs

      if [ -z "$2" ]; then query_user; fi

      rm_warc_count=0
      for w in $orig_warcs
      do
        orig_warc_str=`echo $w | tr '/' ' ' | awk '{print $NF}'`
        t="${w}.tombstone"
        warc_tombstone_str=`echo $t | tr '/' ' ' | awk '{print $NF}'`
        if [ -e $t ]
        then
        echo "+ warc_tombstone: $warc_tombstone_str"
  	echo "  "`cat $t`
  	echo "  original_warc: $orig_warc_str"
  	echo "  rm $w"
	if [ ! -w $w ]
        then
          echo "ERROR: file not writable: $w";
	  continue
        else
  	  rm $w
        fi
  	if [ $? != 0 ]
          then
            echo "ERROR: could not rm file: $w $?"
  	    exit 3
          fi
        fi
        (( rm_warc_count++ ))
      done
      
      total_rm_count=$(( total_rm_count + rm_warc_count ))

      echo "removed ($rm_warc_count) original warcs"

    else
      echo "found NO_TOMBSTONE ($warc_count) warcs in series: $d"
    fi
    warc_count=0
  done
else
  echo "Usage:" `basename $0` $usage
  exit 1
fi

echo "removed ($total_rm_count) original warcs total"

echo `basename $0` "done." `date`
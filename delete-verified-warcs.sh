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

if [ -z "$1" ]; then
    echo "Usage: $(basename $0) $usage"
    exit 1
fi

echo `basename $0` `date`

xfer_job_dir=$1
total_rm_count=0

# loop over warc_series
for d in $(find $1 -mindepth 1 -maxdepth 1 -type d); do

  warc_series=$(basename $d)
  TOMBSTONE="$d/TOMBSTONE"
  MANIFEST="$d/MANIFEST"
  CLEAN_ERR="$d/CLEAN.err"

  if [ -e $CLEAN_ERR ]; then
      echo "$CLEAN_ERR exists - skipping"
      continue
  fi

  # count original warcs in series
  orig_warcs=($(find $d -type f -regex '.*\.w?arc\(\.gz\)?$'))
  warc_count=${#orig_warcs[@]}

  # check for TOMBSTONE
  if [ -e $TOMBSTONE ]; then
    echo "TOMBSTONE exists: $TOMBSTONE"

    tombstone_count=$(wc -l < $TOMBSTONE)
    manifest_count=$(wc -l < $MANIFEST)

    # check manifest, tombstone count
    if [[ $tombstone_count != $manifest_count ]]; then
      echo "ERROR: count mis-match:"\
	   "TOMBSTONE=$tombstone_count "\
	   "MANIFEST=$manifest_count "\
	   "in series: $warc_series" | tee -a $CLEAN_ERR
      continue
    fi

    if [[ $warc_count == 0 ]]; then
      continue
    fi
	
    echo "found ($warc_count) warcs in: $d"

    echo "  tombstone_count: $tombstone_count"
    echo "  manifest_count: $manifest_count"
    echo "  warc_count: $warc_count"

    # remove original warcs

    if [ -z "$2" ]; then query_user; fi

    rm_warc_count=0
    for w in ${orig_warcs[@]}; do
      t="${w}.tombstone"
      if [ -e $t ]; then
	echo "+ warc_tombstone: $(basename $t)"
	echo "  "$(cat $t)
	echo "  original_warc: $(basename $w)"
	echo "  rm $w"
	if [ ! -w $w ]; then
	  echo "ERROR: file not writable: $w";
	  continue
	fi
	rm $w || {
	    echo "ERROR: could not rm file: $w $?" | tee -a $CLEAN_ERR
	    exit 3
	}
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

echo "removed ($total_rm_count) original warcs total"

echo `basename $0` "done." `date`
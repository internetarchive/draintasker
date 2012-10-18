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
cmdname="$(basename $0)"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

function clean_item {
  local d=$1
  local warc_series=$(basename $d)
  local MANIFEST=$d/MANIFEST TOMBSTONE=$d/TOMBSTONE
  local upload_files=($(awk '{print $2}' $MANIFEST))
  local manifest_count=${#upload_files[@]}
  local tombstone_count=$(wc -l < $TOMBSTONE)

  # if MANIFEST and TOMBSTONE does not agree on number of uploaded files,
  # something must have gone wrong (TODO: can we be a bit smarter here?)
  if (( tombstone_count != manifest_count )); then
    printf 'ERROR: count mis-match: TOMBSTONE=%d MANIFEST=%d in series %s\n' \
	$tombstone_count $manifest_count "$warc_series" >&2
    return 1
  fi
  if ((manifest_count == 0)); then
    echo "no uploaded files in this item" >&2
    continue
  fi

  # show what's going to happen if running in interactive mode
  if (( ! nointeractive )); then
    echo "removing $manifest_count files in $d"
    query_user
  fi

  # remove files
  count_removed=0
  count_missing=0
  for w in ${upload_files[@]}; do
    if [ -e $d/$w ]; then
      # files without corresponding .tombstone file should not be
      # deleted.
      t="${w}.tombstone"
      if [ ! -e "$d/$t" ]; then
	# TODO: should we delete TOMBSTONE to have s3-launch-transfer re-process
	# this item?
	echo "ERROR: no .tombstone for $w" >&2
	return 1
      fi
      echo "removing $w uploaded to $(cat $d/$t)" >&2
      rm $d/$w || return 1
      (( count_removed++ ))
    else
      (( count_missing++ ))
    fi
  done
  if (( count_missing > 0 )); then
    echo "$warc_series: removed $count_removed files ($count_missing already removed)" >&2
  else
    echo "$warc_series: removed $count_removed files" >&2
  fi
  (( total_rm_count += count_removed ))
  return 0
}

if [ -z "$1" ]; then
    echo "Usage: $cmdname $usage"
    exit 1
fi

echo "$cmdname: $(date)"

xfer_job_dir=$1
nointeractive=$2
total_rm_count=0

# for reporting: count of active items, inactive items, newly cleaned items
count_items_active=0
count_items_inactive=0
count_items_cleaned=0
count_items_error=0
count_items_locked=0

# loop over warc_series
for d in $(find $xfer_job_dir -mindepth 1 -maxdepth 1 -type d); do

  iid=$(basename $d)
  TOMBSTONE="$d/TOMBSTONE"
  MANIFEST="$d/MANIFEST"
  CLEAN="$d/CLEAN"

  if [ -e $CLEAN.err ]; then
    echo "$iid: has CLEAN.err"
    continue
  fi
  if [ -e $CLEAN ]; then
    (( count_items_inactive++ ))
    continue
  fi

  # s3-launch-transfer.sh creates TOMBSTONE file when it has successfully
  # uploaded all files. if TOMBSTONE file does not exist, the item is
  # still being worked on.
  if [ ! -e $TOMBSTONE ]; then
    echo "$iid: no TOMBSTONE yet"
    (( count_items_active++ ))
    continue
  fi

  if [ -e $CLEAN.open ]; then
    echo "$iid: has $CLEAN.open"
    (( count_items_locked++ ))
    continue
  fi

  if clean_item $d 2>${CLEAN}.open; then
    mv ${CLEAN}.open $CLEAN
    (( count_items_cleaned++ ))
  else
    mv ${CLEAN}.open ${CLEAN}.err
    echo failed to clean $d - see ${CLEAN}.err
    tail -1 ${CLEAN}.err
    (( count_items_error++ ))
  fi
done

printf '%s: %d cleaned, %d inactive, %d active, %d error, %d locked\n' \
    "$cmdname" \
    $count_items_cleaned $count_items_inactive $count_items_active \
    $count_items_error $count_items_locked
echo "$cmdname: removed $total_rm_count files total"
echo "$cmdname: done $(date)"
#!/bin/bash
# delete-verified-warcs.sh xfer_job_dir [force]
#
# for each warc_series, if SUCCESS found, find each 
# original_warc in warc_series, and if matching .tombstone file
# found and tombstone_count and manifest_count agree, then 
# delete original_warc.
#
#  xfer_job_dir    /{path}/{job_name}
#  warc_series     {xfer_job_dir}/{warc_series}
#  SUCCESS         {warc_series}/SUCCESS
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
  local MANIFEST=$d/MANIFEST
  local upload_files=($(awk '{print $2}' $MANIFEST))
  local manifest_count=${#upload_files[@]}

  # first double check if all files listed in MANIFEST have been uploaded.
  # if .tombestone's and MANIFEST don't agree, sometthing must have gone
  # wrong. need to call operator's attention.
  local missedout=()
  for w in ${upload_files[@]}; do
      if [ ! -e $d/$w.tombstone ]; then
	  missedout+=("$w")
      fi
  done
  if (( ${#missedout[@]} != 0 )); then
      for w in ${missedout[@]}; do
	  echo "$w: listed in MANIFEST, but no .tombstone exists" >&2
      done
      printf 'ERROR: %d file(s) not uploaded while SUCCESS exists\n' \
	  ${#missedout[@]} >&2
      return 1
  fi
  if ((manifest_count == 0)); then
    echo "no uploaded files in this item" >&2
    return 0
  fi

  # show what's going to happen if running in interactive mode
  if (( ! nointeractive )); then
    echo "removing $manifest_count files in $d"
    query_user
  fi

  echo "cleaning $warc_series"
  # remove files
  count_removed=0
  count_missing=0
  for w in ${upload_files[@]}; do
    if [ -e $d/$w ]; then
      # files without corresponding .tombstone file should not be
      # deleted. this is already checked above, but just double-checking.
      t="${w}.tombstone"
      if [ ! -e "$d/$t" ]; then
	# TODO: should we delete SUCCESS etc. to have s3-launch-transfer
	# re-process this item?
	echo "ERROR: no .tombstone for $w" >&2
	return 1
      fi
      echo "removing $w uploaded to $(cat $d/$t)" >&2
      rm $d/$w || return 1
      (( count_removed++ ))
    else
      # missing (already deleted) files are okay. this shouldn't happen under
      # normal situation, but it is sometimes necessary to add/re-upload files
      # to the item after upload is complete.
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
  SUCCESS="$d/SUCCESS"
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

  # s3-launch-transfer.sh creates SUCCESS file when it has successfully
  # uploaded all files. if SUCCESS file does not exist, the item is
  # still being worked on or on hold for some error.
  if [ ! -e $SUCCESS ]; then
    echo "$iid: no SUCCESS yet"
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

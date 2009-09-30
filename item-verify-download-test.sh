#!/bin/bash 
# item-verify-download-test.sh local_item_dir task_id [verbose]
#
# wgets task_log, checks task_id against TASK file, gets 
# remote_warc_url from task_log and associated filesxml(s) 
# for each local_warc, wgets remote_warc_url into tmp_warc, 
# computes tmp_warc_md5 and checks against manifest_md5, 
# exits on error, or leaves warc_tombstone for each verified 
# local_warc and series TOMBSTONE file containing 
# remote_warc_urls for all local_warcs.
#
# Note: currently, there may be more than one remote_item per 
#       local manifest
#
#  local_item_dir   contains warcs to be transferred (aka warc_series)
#  task_id          catalog task_id
#  task_log         catalog task log log_show.php?task_id=task_id
#  remote_item      item container on remote storage
#  filesxml         remote_item XML file containing filename, md5 
#  remote_warc_url  http access url to remote warc
#  tmp_warc         /tmp/warc_file
#  tmp_warc_md5     md5sum of tmp_warc
#  manifest_md5     md5sum of local_warc from local_item_dir/MANIFEST
#  local_warc       local_item_dir/*.w/arc.gz
#  warc_tombstone   local_warc.tombstone containing remote_warc_url
#  TOMBSTONE        listing of series *.tombstone
#
#  see also
#    get_remote_warc_urls.sh
#
# siznax 2009

usage="$0 local_item_dir task_id [verbose]"

if [ -n "$2" ]
then

  echo $0 `date`

  local_item_dir=$1
  task_id=$2 
  back=`pwd`

  TASK="$local_item_dir/TASK"
  PACKED="$local_item_dir/PACKED"
  MANIFEST="$local_item_dir/MANIFEST"
  open="$local_item_dir/VERIFYING.open"
  ERROR="$local_item_dir/ERROR"
  TOMBSTONE="$local_item_dir/TOMBSTONE"

  task_url="http://www.us.archive.org/log_show.php?task_id=${task_id}"
  task_log="/tmp/task_log-${task_id}"
  get_remote_warc_urls=`pwd`"/get-remote-warc-urls.sh"

  # get task_log
  wget -q $task_url -O $task_log
  if [ $? != 0 ]; then
    echo "ERROR: failed to wget task_log: $task_url"
    exit 2
  fi

  # check manifest count
  manifest_count=`wc -l $MANIFEST | awk '{print $1}'`
  packed_count=`cat $PACKED | awk '{print $2}'`

  # check task number 
  task_file_task_id=`grep '^task_id=' $TASK | tr "=" " " | awk '{print $2}'`
  if [ "$task_file_task_id" != "$task_id" ]
  then
    echo "ERROR: task_id ($task_id) differs from task file: $task_file_task_id"
    exit 3
  fi
  
  rhost=`grep '^thumper=' $TASK | tr "=" " " | awk '{print $NF}'`
  warc_series=`echo $local_item_dir | egrep -o '/([^/]*)$' | tr -d "/"`

  echo "  local_item_dir : $local_item_dir"
  echo "  warc_series    : $warc_series"
  echo "  task_id        : $task_id"
  echo "  task_log       : $task_log"
  echo "  rhost          : $rhost"

  if [ -e $open ]; then
    echo "OPEN file exists: $open"
    exit 0
  fi

  if [ -e $TOMBSTONE ]; then
    echo "TOMBSTONE file exists: $TOMBSTONE"
    exit 0
  fi

  start_verification=`date -u +%Y-%m-%dT%H:%M:%SZ`
  echo "Began verifying $warc_series $start_verification"

  echo "opening file: $open"
  # touch $open
  if [ $? != 0 ]; then
    echo "ERROR: could not touch file: $open"
    exit 4
  fi

  # get remote_warc_urls (split) and put into tmp
  echo "get_remote_warc_urls: $task_log"
  remote_warc_urls_tmp="/tmp/files-${task_id}"
  `$get_remote_warc_urls $task_log > $remote_warc_urls_tmp`
  if [ $? != 0 ]
  then
    echo "ERROR: get_remote_warc_urls failed with status: $?"
    exit 9
  fi
  echo "remote_warc_urls_tmp: $remote_warc_urls_tmp"
  cat $remote_warc_urls_tmp

  # foreach local_warc checksum remote_warc
  cd $local_item_dir
  warc_count=`ls *.{arc,warc}.gz | wc -l`
  for f in `ls *.{arc,warc}.gz`
  do

    local_warc="$local_item_dir/$f"
    remote_warc_url=`grep $f $remote_warc_urls_tmp | awk '{print $3}'`
    tmp_warc="/tmp/$f"

    warc_tombstone="${local_warc}.tombstone"
    warc_name=`echo $f | tr "." " " | awk '{print $1}'`

    # previously verified if tombstone exists
    if [ -e $warc_tombstone ]
    then
      echo "REMOTE_CHECKSUM_OK:" `echo $warc_tombstone | grep -o '[^/]*$'`
      # cat $warc_tombstone >> $open
      continue
    fi

    # download remote warc into /tmp
    echo "wget -q $remote_warc_url -O $tmp_warc"
    # wget -q $remote_warc_url -O $tmp_warc
    if [ $? != 0 ]; then
      err="ERROR: failed to wget remote_warc_url: $remote_warc_url"
      echo $err
      # echo $err >> $open
      # mv $open $ERROR
      exit 5
    fi

    # local_md5
    manifest_md5=`grep $f $MANIFEST | awk '{print $1}'`

    # compute tmp_warc MD5
    # <TEST>
    # tmp_md5=`md5sum $tmp_warc | awk '{print $1}'`
    tmp_md5=$manifest_md5
    # </TEST>

    # verbose output
    if [ "$3" ]
    then
      echo "  warc_name   : $warc_name"
      echo "  local_warc  : $local_warc"
      echo "  manifest_md5: $manifest_md5"
      echo "  remote_warc : $remote_warc_url"
      echo "  tmp_md5     : $tmp_md5"
    fi

    # compare checksum
    if [ "$tmp_md5" != "$manifest_md5" ]; then
      err="ERROR: BAD_CHECKSUM $tmp_md5 $f"
      echo $err
      # echo $err >> $open
      # mv $open $ERROR
      exit 6
    fi

    # leave tombstone
    echo "REMOTE_CHECKSUM_OK $tmp_md5 $warc_name"

    # echo $remote_warc_url > $warc_tombstone
    # echo $remote_warc_url >> $open

    # remove tmp warc
    # rm $tmp_warc
    if [ $? != 0 ]; then
      err="ERROR failed to rm tmp_warc: $tmp_warc"
      echo $err
      # echo $err >> $open
      # mv $open $ERROR
      exit 7
    fi

    tmp_warc=''
    remote_warc_url=''

  done

  # <TEST>
  # tombstone_count=`ls $local_item_dir/*.tombstone | wc -l`
  tombstone_count=$warc_count
  # </TEST>

  # check counts
  if [ $warc_count != $packed_count ] ||
     [ $warc_count != $manifest_count ] ||
     [ $warc_count != $tombstone_count ] 
  then
    err="ERROR: count mismatch: $packed_count packed $manifest_count manifest $tombstone_count tombstone"
    echo $err
    # echo $err >> $open
    # mv $open $ERROR
    exit 8
  else
    echo "verified $warc_count warcs $packed_count packed $manifest_count manifest $tombstone_count tombstone"
    # mv $open $TOMBSTONE
    echo "wrote $TOMBSTONE"
  fi

  finish_verification=`date -u +%Y-%m-%dT%H:%M:%SZ`
  echo "Done verifying $warc_series $start_verification $finish_verification"

  rm $remote_warc_urls_tmp
  rm $task_log
  cd $back

else 
  echo $usage
  exit 1
fi

echo $0 "Done." `date`
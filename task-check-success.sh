#!/bin/bash 
# task-check-success task_id
#
# get task_log, look for SUCCESS_STRING, and FILES_XML 
# for each item. echo "task_id SUCCESS_STRING" and 
# "FILES_XML_RECONCILED" for each remote_item and 
# return 0 when CONDITIONS met
#
#  SUCCESS_STRING  "TASK FINISHED WITH SUCCESS"
#  task_log        task_url(task_id)
#  
#
#
# siznax 2009

usage="$0 task_id"

if [ -n "$1" ]
then
  
  task_id=$1
  success_str='TASK FINISHED WITH SUCCESS'
  task_url="http://www.us.archive.org/log_show.php?task_id=$task_id"
  task_log="/tmp/task_log-$task_id"

  # get task_log
  wget -q $task_url -O $task_log
  if [ $? == 0 ]; then
    g=`grep -o "$success_str" $task_log`
    if [ "$g" == "$success_str" ]
    then
      task_status="$task_id "`echo $g | tr " " "_"`
    else
      echo "ERROR: success string not found in task_log: $task_url"
      exit 2
    fi
  else
    echo "ERROR: failed to wget task_log: $task_log with status: $?"
    exit 3
  fi

  # time
  start=`grep 'Task started at: UTC: .* (' $task_log | awk '{print $5"Z"$6}' | head -1`
  finish=`egrep 'TASK FINISHED WITH SUCCESS at UTC: .*,' $task_log | tr -d "," | awk '{print $7"Z"$8}'`

  # get filesxml(s)
  rhost=`grep '\[to_server\]' $task_log | uniq | awk '{print $NF}'`
  manifest=`grep '\[prefix\]' $task_log | uniq | awk '{print $NF}'`"/MANIFEST"
  filesxml_count=0
  checksum_ok_count=0
  for filesxml in `grep "^[^echo].*/.*_files.xml" $task_log | uniq`
  do
    filesxml_url="http://${rhost}/0/items/${filesxml}"
    filesxml_tmp="/tmp/filesxml-${task_id}-${filesxml_count}"
    echo "  filesxml_url = $filesxml_url"
    echo "  filesxml_tmp = $filesxml_tmp"
    # wget filesxml
    wget -q $filesxml_url -O $filesxml_tmp
    if [ $? != 0 ]; then
      echo "ERROR: failed to wget filesxml: $filesxml_url with status: $?"
      exit 4
    fi

    remote_file=''
    remote_md5=''

    # checksum
    for l in `cat $filesxml_tmp`
    do
      if [[ "$l" =~ "^name=" ]]
      then
        remote_file=`echo $l | tr -d \" | tr "=" " " | awk '{print $NF}'`
      fi
      if [[ "$l" =~ "\<md5\>.*\<md5\>" ]]
      then
        remote_md5=`echo $l | grep -o '>.*<' | tr -d ">" | tr -d "<"`
      fi
      if [ -n "$remote_file" ] && [ -n "$remote_md5" ]
      then
        local_md5=`grep $remote_file $manifest | awk '{print $1}'`
	if [ "$remote_md5" == "$local_md5" ]
        then
	  echo "REMOTE_CHECKSUM_OK: $remote_md5 $remote_file"
	  (( checksum_ok_count++ ))
        else 
          echo "BAD_REMOTE_CHECKSUM: $remote_md5 $remote_file"
	  exit 5
        fi
        remote_file=''
	remote_md5=''
	local_md5=''
      fi
    done

    (( filesxml_count++ ))

    rm $filesxml_tmp

  done

  if [ $filesxml_count == 0 ]
  then
    echo "ERROR: FILESXML not found in task_log: $task_url" 
    exit 6
  fi

  rm $task_log

  echo "$task_status $start $finish $filesxml_count FILESXML $checksum_ok_count CHECKSUM_OK"
  exit 0

else 
  echo $usage
  exit 1
fi


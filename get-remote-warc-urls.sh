#!/bin/bash
# get-remote-warc-urls tmp_task_log
#
# finds filesxml entries in a tmp_task_log, wgets each
# filesxml_url into filesxml_tmp, finds name and md5
# foreach file, and outputs [file md5 url]
#
# tmp_task_log  task_log previously downloaded into /tmp
# filesxml      "*_files.xml" in tmp_task_log
# filesxml_url  constructed filesxml url
# filesxml_tmp  /tmp/filesxml-{task_id}-{filesxml_count}
# output        file remote_md5 remote_url
#
# siznax 2009

usage="$0 tmp_task_log"

if [ -n "$1" ]
then

if [ ! -e $1 ] 
then
  echo "TMP_TASK_LOG not found: $1"
else
  task_log=$1
fi

task_id=`grep '\[task_id\]' $task_log | tr -d "[" | tr -d "]" | awk '{print $3}' | tr -d "\n"`
rhost=`grep '\[to_server\]' $task_log | uniq | awk '{print $NF}'`

file_count=0
filesxml_count=0
checksum_ok_count=0

for filesxml in `grep "^[^echo].*/.*_files.xml" $task_log | uniq`
do

  (( filesxml_count++ ))

  remote_item=`echo $filesxml | tr '/' ' ' | awk '{print $((NF-1))}'`
  filesxml_url="http://${rhost}/0/items/${filesxml}"
  filesxml_tmp="/tmp/filesxml-${task_id}-${filesxml_count}"

  echo "==== $filesxml ===="
  echo "  remote_item = $remote_item"
  echo "  filesxml_url = $filesxml_url"
  echo "  filesxml_tmp = $filesxml_tmp"

  # get filesxml
  echo "wget -q $filesxml_url -O $filesxml_tmp"
  wget -q $filesxml_url -O $filesxml_tmp
  if [ $? != 0 ]
  then
    echo "ERROR: failed to wget filesxml: $filesxml_url with status: $?"
    exit 1
  fi

  # get remote [md5 url] from filesxml
  for l in `cat $filesxml_tmp`
  do

    if [[ "$l" =~ "name" ]]
    then
      remote_file=`echo $l | tr -d \" | tr "=" " " | awk '{print $NF}'`
    fi

    if [[ "$l" =~ "md5" ]]
    then
      remote_md5=`echo $l | grep -o '>.*<' | tr -d ">" | tr -d "<"`
    fi

    remote_url="http://${rhost}/0/items/${remote_item}/${remote_file}"

    if [ -n "$remote_file" ] && [ -n "$remote_md5" ]
    then

      # DEBUG
      # echo " "
      # echo $l
      # echo "  remote_file: " $remote_file 
      # echo "  remote_md5:  " $remote_md5
      # echo "  remote_url:  " $remote_url

      (( file_count++ ))
      echo "$remote_file $remote_md5 $remote_url"
      remote_file=''
      remote_md5=''
      remote_url=''
    fi
  done

done

echo "task $task_id remote_warcs_urls $file_count files $filesxml_count filesxml"
exit 0

else 
  echo $usage
  exit 1
fi

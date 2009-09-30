#!/bin/bash 
# verify-transfers xfer_job_dir method mode [verbose]
# 
# find TASK files in xfer_job_dir, get task_id from TASK file, 
# run CHECK for task success and run VERIFY on success
#
#  xfer_job_dir  contains warc_series mayhaps transferred
#  warc_series   {xfer_job_dir}/{warc_series}
#  TASK          {xfer_job_dir}/{warc_series}/TASK
#  task_id       catalog task_id
#  CHECK         task-check-success.sh
#  VERIFY        item-verify-size.sh | item-verify-download.sh
#  method        download = wget remote warcs to /tmp for checksum
#                size = curl remote warcs for size only
#  mode          all = verify all series
#                single = verify 1 series, then exit (no waiting!)
#  see also      get_remote_warc_urls.sh
#
# siznax 2009

verify_count=0

if [ -n "$3" ]
then

  echo $0 `date`

  check_task_success="./task-check-success.sh"

  if [ "$2" == 'download' ]
  then
    verify_remote_warcs="./item-verify-download.sh"
  elif [ "$2" == 'size' ]
  then
    verify_remote_warcs="./item-verify-size.sh"
  else
    echo "ERROR: unrecognized method: $2"
    exit 1
  fi

  if [ "$3" == 'all' ]
  then
    mode='all'
  elif [ "$3" == 'single' ]
  then
    mode='single'
  else
    echo "ERROR: unrecognized mode: $3"
    exit 1
  fi

  for d in `find $1 -type d | sort`
  do

    warc_series=`echo $d | egrep -o '/([^/]*)$' | tr -d "/"`

    if [ -z $warc_series ]; then continue; fi

    TASK="$d/TASK"
    SUCCESS="$d/SUCCESS"
    TOMBSTONE="$d/TOMBSTONE"
    ERROR="$d/ERROR"
    VERIFYING_OPEN="$d/VERIFYING.open"

    if [ "$4" ]
    then
      echo "verifying warc_series: $warc_series"
      echo "  TASK: $TASK"
      echo "  OPEN: $VERIFYING_OPEN"
      echo "  TOMBSTONE: $TOMBSTONE"
      echo "  check: $check_task_success"
      echo "  verify: $verify_remote_warcs" 
    fi

    # skip series currently being verified
    if [ -e $VERIFYING_OPEN ]
    then
      echo "OPEN file exists: $VERIFYING_OPEN"
      continue
    fi

    # do not attempt to re-verify
    if [ -e $TOMBSTONE ]
    then
      echo "TOMBSTONE file exists: $TOMBSTONE"
      continue
    fi

    # skip previous ERRORs
    if [ -e $ERROR ]
    then
      echo "ERROR file exists: $ERROR"
      continue
    fi

    # begin verification
    if [ -e $TASK ]; then

      echo "====" $TASK "===="
      task_id=`egrep '^task_id=[0-9]+' $d/TASK | tr "=" " " | awk '{print $NF}'`
      if [ -z "$task_id" ]; then
        echo "INVALID_TASK_ID"
        continue
      fi

      # check task status
      echo "check_task_success $task_id"
      task_status=`$check_task_success $task_id`

      if [ $? == 0 ]
      then

        echo "$task_status" > $SUCCESS
        cat $SUCCESS | tail -1

        # verify transfer 
        echo "verify_remote_warcs $d $task_id"
	$verify_remote_warcs $d $task_id 1

	(( verify_count++ ))

        # check mode
        if [ $mode == 'single' ]
        then
          echo "$verify_count series verified"
          echo "mode = $mode, exiting normally."
          exit 0
        fi

      else

        echo $task_id "TASK SUBMITTED: $TASK"

      fi
    fi
 
    task_id=''
    task_status=''

  done

else 
  # usage
  echo "$0 xfer_job_dir method mode [verbose]"
  echo "  method download = wget remote warcs to /tmp for checksum"
  echo "         size = curl remote warcs for size only"
  echo "  mode   all = verify all series"
  echo "         single = verify 1 series, then exit (no waiting!)"
  exit 1
fi

echo "$verify_count series verified"
echo $0 "Done." `date`
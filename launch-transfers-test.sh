#!/bin/bash 
# launch-transfers.sh xfer_job_dir thumper [force] [mode]
#
# foreach each item_dir in xfer_job_dir, checks for LAUNCH.open
# and TASK, and if not found, then forms task_args to submit a 
# catalog task to begin transfers to remote storage 
#
#  xfer_job_dir   /{rsync_path}/{JOB}
#  xfer_item_dir  /{rsync_path}/{JOB}/{warc_series}
#  warc_series    group of warcs limited by size or count (e.g. 10G)
#  task_args      manifest crawldata prefix thumper
#  manifest       "rsync://{host}/{rsync_module}/{manifest_path}"
#  crawldata      path to xfer_item_dir
#  prefix         item_name prefix, e.g. {warc_series}
#  thumper        destination storage host
#  [force]        optionally skip interactive continue
#  [mode]         single = submit 1 task, then exit
#
# DEPENDENCIES
#
#  PETABOX_HOME   e.g. '/home/webcrawl/petabox'
#
# siznax 2009

usage="$0 xfer_job_dir thumper [force] [mode=single]"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

launch_count=0

if [ -n "$2" ]
then

  if [ -n "$4" ]
  then
    mode=$4
  else
    mode=0
  fi

  echo $0 `date`

  xfer_job_dir="$1"
  thumper="$2"

  back=`pwd`
  cd $1

  home='home.us.archive.org'
  host=`hostname`
  rsync_dir="incoming_3"
  submit="$PETABOX_HOME/sw/bin/thumper_submit_stub.php"

  echo "  home     : $home"
  echo "  host     : $host"
  echo "  rsync_dir: $rsync_dir"
  echo "  submit   : $submit"

  for d in `find $xfer_job_dir -type d | sort`
  do

    MANIFEST="$d/MANIFEST"
    if [ -e "$MANIFEST" ]; then

      OPEN="$d/LAUNCH.open"
      LAUNCH="$d/LAUNCH"
      TASK="$d/TASK-test"
      warc_series=`echo $d | egrep -o '/([^/]*)$' | tr -d "/"`

      crawldata="$d"
      prefix=$warc_series

      # check for lock (open file) on this process
      if [ -e $OPEN ]; then
        echo "OPEN file exists: $OPEN"
        continue
      fi

      # don't re-submit the same task
      if [ -e "$d/TASK" ]; then
        echo "TASK file exists: $d/TASK"
        continue
      fi

      # check manifest
      warc_count=`ls $d/*.{arc,warc}.gz | wc -l`
      manifest_count=`cat $MANIFEST | wc -l`
      if [ $manifest_count != $warc_count ] 
      then
        echo "ERROR: BAD MANIFEST: warc_count=$warc_count manifest_count=$manifest_count"
        exit 1
      fi
      
      echo "==== $warc_series ===="

      # build manifest_rsync_url      
      manifest_path=`echo $MANIFEST\
       | tr '/' " "\
       | awk '{print $((NF-2))"/"$((NF-1))"/"$NF}'`
      manifest_rsync_url="rsync://${host}/${rsync_dir}/${manifest_path}"

      echo "opening file: $OPEN"
      echo "task output file: $TASK"
      echo "  submit    = $submit"
      echo "  manifest  = $manifest_rsync_url"
      echo "  crawldata = $crawldata"
      echo "  prefix    = $prefix"
      echo "  thumper   = $thumper"
      echo "  mode      = $mode"
      echo "ssh ${home} submit manifest crawldata prefix thumper"

      if [ -z "$3" ]
      then
        query_user
      fi

      # write LAUNCH file
      echo "submit=$submit" > $OPEN
      echo "manifest=$manifest_rsync_url" >> $OPEN
      echo "crawldata=$crawldata" >> $OPEN
      echo "prefix=$prefix" >> $OPEN
      echo "thumper=$thumper" >> $OPEN

      # launch task
      sleep 2
      task_output=`ssh ${home} ${submit} ${manifest_rsync_url} ${crawldata} ${prefix} ${thumper}`
      if [ $? != 0 ]; then
        echo "ERROR: submit failed with output:" $task_output
        exit 1
      else 
        echo "writing task_output to TASK file: $TASK"
        echo $task_output | tr " " "\n" > $TASK
	if [ $? == 0 ]; then
          cat $TASK | tr " " "\n  "
	else
	  echo "ERROR: could not write file: $TASK"
	  exit 1
	fi
      fi 

      # unlock process
      mv $OPEN $LAUNCH
      if [ $? != 0 ]
      then
        echo "ERROR: failed to mv $OPEN to $LAUNCH"
        exit 1
      else
        echo "mv open file to LAUNCH: $LAUNCH"
      fi

      (( launch_count++ ))

      # check mode
      if [ $mode == 'single' ]
      then
        echo "$launch_count tasks submitted"
        echo "mode = $mode, exiting normally."
        exit 0
      fi

    fi # /if [ -e MANIFEST ]

    OPEN=''
    LAUNCH=''
    TASK=''
    task_output=''

  done

  cd $back

else 
  echo $usage
  exit 1
fi

echo "$launch_count tasks submitted"
echo $0 "Done." `date`


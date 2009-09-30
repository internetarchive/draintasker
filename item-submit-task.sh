#!/bin/bash
# item-submit-task xfer_item_dir [submit]
#
# launches submit with args from LAUNCH file
# 
#  xfer_item_dir dir containing warcs to be transferred
#  submit        PETABOX_HOME/thumper_submit_warc_series.php
#  stub          PETABOX_HOME/thumer_submit_stub.php
#  LAUNCH        contains submit args
#                manifest  manifest_rsync_url
#                crawldata transfer item dir
#                prefix    identifier prefix
#                thumper   destination
#  [submit]      uses submit task when 1, else stub
#   
# siznax 2009

usage="$0 xfer_item_dir [submit]"

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
  xfer_item_dir="$1"
  LAUNCH="$xfer_item_dir/LAUNCH"
  thumper_script="$PETABOX_HOME/sw/bin/thumper_submit_warc_series.php"
  stub="$PETABOX_HOME/sw/bin/thumper_submit_stub.php"
  home="home.us.archive.org"

  if [ -e $LAUNCH ]
  then
    manifest=`grep '^manifest=' $LAUNCH | tr "=" " " | awk '{print $NF}'`
    crawldata=`grep '^crawldata=' $LAUNCH | tr "=" " " | awk '{print $NF}'`
    prefix=`grep '^prefix=' $LAUNCH | tr "=" " " | awk '{print $NF}'`
    thumper=`grep '^thumper=' $LAUNCH | tr "=" " " | awk '{print $NF}'`
    if [ "$2" == "1" ]
    then
      submit=$thumper_script
      TASK="$xfer_item_dir/TASK"
    else
      submit=$stub
      TASK="$xfer_item_dir/TASK-test-$$"
    fi

    # check manifest
    warc_count=`ls $xfer_item_dir/*.{arc,warc}.gz | wc -l`
    manifest_count=`cat $xfer_item_dir/MANIFEST | wc -l`
    if [ $manifest_count != $warc_count ] 
    then
      echo "BAD MANIFEST: warc_count=$warc_count manifest_count=$manifest_count"
      exit 1
    fi
      
    # do not re-submit the same task
    if [ -e $TASK ]
    then 
      echo "TASK file exists: $TASK"
      exit 0
    fi

    echo $LAUNCH
    echo "  submit    = $submit"
    echo "  manifest  = $manifest"
    echo "  crawldata = $crawldata"
    echo "  prefix    = $prefix"
    echo "  thumper   = $thumper"
    echo "  TASK      = $TASK"
    query_user

    # launch task
    sleep 2
    echo "ssh home submit manifest crawldata prefix thumper > TASK"
    output=`ssh $home $submit $manifest $crawldata $prefix $thumper > $TASK`
    cat $TASK
    exit 0

  else 
    echo "LAUNCH file not found: $LAUNCH"
    exit 2
  fi
else
  echo $usage
  exit 1
fi

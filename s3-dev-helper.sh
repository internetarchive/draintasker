#!/bin/bash
#
#  USE AT YOUR OWN RISK! this script could seriously fuckup 
#  your data and other systems, so pay attention!
#
#  s3-dev-helper.sh warc_series token mode
#
#    warc_series  fqpn to warc_series
#    token        string for prefix or suffix
#    mode         prefix or suffix 
# 
#  use this script to move aside SUCCESSfully run files, so that
#  the underlying warcs may be resubmitted verbatim for testing 
#  purposes. it will attempt to move aside the following critical 
#  files:  LAUNCH SUCCESS TASK TOMBSTONE
#
# siznax 2010

usage="s3-dev-helper.sh warc_series token mode"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]; then echo "Aborting."; exit 1; fi
}

if [ -z "$3" ]
then

    echo $usage
    exit 1

else

    warc_series=$1
    token=$2
    mode=$3

    # if no warcs found, cannot cleanup
    warc_count=0
    for warc in `find $warc_series -name *.warc.gz`
    do
        (( warc_count++ ))
    done
    if [ $warc_count == 0 ]
    then
        echo "ERROR: no warcs found, cannot cleanup."
	exit 2
    fi

    # remove tombstones
    echo "finding tombstones:"
    ts_count=0
    for ts in `find $warc_series -name *.tombstone`
    do
	echo "  $ts"
	tombstones[$ts_count]=$ts
        (( ts_count++ ))
    done

    if [ ${#tombstones[@]} -gt 0 ]
    then
	echo "removing tombstones:"
        query_user
        for (( i = 0 ; i < ${#tombstones[@]} ; i++ ))
        do
            rm -i ${tombstones[$i]}
        done
    else
        echo "no tombstones"
    fi

    # rename files
    for file in LAUNCH SUCCESS TASK TOMBSTONE
    do
	if [ -f $warc_series/$file ]
        then
            if [ $mode == "prefix" ]
            then
                aside="${token}${file}"
            elif [ $mode == "suffix" ]
            then
                aside="${file}${token}"
            else
                echo "ERROR: un-recognized mode: $mode"
		exit 3
            fi
        echo "mv $warc_series/$file "
	echo "   $warc_series/$aside"
	query_user
	mv $warc_series/$file $warc_series/$aside
	fi
    done

fi




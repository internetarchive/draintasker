#!/bin/bash
# eject_tombstones.sh
#   move dirs containing TOMBSTONE files to done dir
# siznax 2010

if [ $# -lt 1 ] 
then
    echo "Usage:" `basename $0` "xfer_dir"
else
    xfer_dir=$1
    done_dir="${1}_done"
fi
if [ ! -d $xfer_dir ]
then
    echo "ERROR: xfer_dir not found:" $xfer_dir
    exit 1
fi
if [ ! -d $done_dir ]
then
    echo "mkdir $done_dir"
    mkdir $done_dir
    if [ $? != 0 ]
    then
	echo "ERROR: could create done_dir:" $done_dir
    fi
fi
for f in `find $xfer_dir -name "TOMBSTONE"`
do 
    d=`echo $f | sed -e 's/\/[^\/]*$//'` 
    echo "mv $d $done_dir"
    mv $d $done_dir
done

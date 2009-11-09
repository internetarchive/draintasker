#!/bin/bash
#
# copy verbatim all crawldata artifacts from all drives on source host
# into a single directory on a destination (staging) host, preserving
# exactly the original directory structure of copied crawldata.
#
# copies from
#   localhost:{[0,1,2,3]}/{jobdir,crawldata,incoming}/{crawlname}
# to
#   dest_host:/0/crawldata/{[0,1,2,3]}/{jobdir,crawldata,incoming}/{crawlname}
#
#  crawlname  name of crawl job, e.g. GEOCITIES-TEST
#  jobsdir    crawl job directory, e.g. /0/closure-jobs
#  dest_host  hostname of staging machine
#
# see also:
#   check-crawldata.sh
#   check-crawldata-staged.sh
#   bundle-crawl-artifacts.sh
# examples:
#   /home/webcrawl/scripts/copy-geo-crawldata.sh
#
# siznax 2009

if [ $# != 3 ]
then
  echo "Usage: $0 crawlname jobsdir dest_host"
  exit 1
fi

crawlname=$1
jobsdir=$2
dest_host=$3
src_host=`hostname | cut -d '.' -f 1`

# HACK!
if [ "$src_host" == "hadoop-127" ]
then
  src_host="ia400130"
fi

dest_dir="/0/crawldata/${crawlname}-${src_host}"
jobdir=${jobsdir}/${crawlname}

echo $0 `date`

if [ ! -d ${jobdir} ]
then
  echo "ERROR job not found:" ${jobdir}
  exit 2
fi

src[0]=${jobsdir}/${crawlname}
src[1]=/1/crawldata/${crawlname}
src[2]=/2/crawldata/${crawlname}
src[3]=/3/crawldata/${crawlname}
src[4]=/0/incoming/${crawlname}
src[5]=/1/incoming/${crawlname}
src[6]=/2/incoming/${crawlname}
src[7]=/3/incoming/${crawlname}

dest_sub[0]=${jobsdir}
dest_sub[1]=/1/crawldata
dest_sub[2]=/2/crawldata
dest_sub[3]=/3/crawldata
dest_sub[4]=/0/incoming
dest_sub[5]=/1/incoming
dest_sub[6]=/2/incoming
dest_sub[7]=/3/incoming

echo "source dirs"
i=0
j=0
for d in "${src[@]}"
do
  if [ -d $d ]
  then
    echo " " $d
    src_dirs[$i]=$d
    dest_subdirs[$i]=${dest_sub[$j]}
    ((i++))
  fi
  ((j++))
done

k=0
for src_dir in "${src_dirs[@]}"
do

  echo "ssh ${dest_host} mkdir -p ${dest_dir}${src_dir}"
  ssh ${dest_host} mkdir -p ${dest_dir}${src_dir}

  echo "scp -pqr ${src_dir} ${dest_host}:${dest_dir}${dest_subdirs[$k]}"
  scp -pqr ${src_dir} ${dest_host}:${dest_dir}${dest_subdirs[$k]}

  if [ $? != 0 ]
  then
    echo "ERROR scp failed with status: $?"
  fi

  ((k++))

done
 echo "Done." `date`
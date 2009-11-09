#!/bin/bash
#
# scan crawldirs contained in stagingdir for a particular job and
# report numfiles and sizedir. can be used via ssh to compare staged
# crawldata (on the localhost) to the orig_host crawldata.
#
#  stagindir  top-level staging dir is "/0/crawldata"
#  jobdir     {stagingdir}/${crawlname}-{orig_host}
#  crawlname  name of crawl job, e.g. GEOCITIES-TEST
#  jobsdir    crawl job directory, e.g. /0/closure-jobs
#  orig_host  hostname containing original crawldata
#
#  numfiles   number of files in each crawldir as reported by "find"
#  sizedir    size of crawldir as reported by "du"
#
# see also:
#   check-crawldata.sh
# examples:
#   /home/webcrawl/scripts/check-geo-staging.sh
#
# siznax 2009

if [ $# != 3 ]
then
  echo "Usage: $0 crawlname jobsdir orig_host"
  exit 1
fi

host=`hostname | cut -d '.' -f 1`
crawlname=$1
jobsdir=$2
orig_host=$3
jobdir=/0/crawldata/${crawlname}-${orig_host}${jobsdir}/${crawlname}

if [ ! -d ${jobdir} ]
then
  echo "ERROR job not found:" ${jobdir}
  exit 2
fi

dir[0]=${jobdir}
dir[1]=/0/crawldata/${crawlname}-${orig_host}/1/crawldata/${crawlname}
dir[2]=/0/crawldata/${crawlname}-${orig_host}/2/crawldata/${crawlname}
dir[3]=/0/crawldata/${crawlname}-${orig_host}/3/crawldata/${crawlname}
dir[4]=/0/crawldata/${crawlname}-${orig_host}/0/incoming/${crawlname}
dir[5]=/0/crawldata/${crawlname}-${orig_host}/1/incoming/${crawlname}
dir[6]=/0/crawldata/${crawlname}-${orig_host}/2/incoming/${crawlname}
dir[7]=/0/crawldata/${crawlname}-${orig_host}/3/incoming/${crawlname}

i=0
for d in "${dir[@]}"
do
  if [ -d $d ]
  then
    crawldirs[$i]=$d
    ((i++))
  fi
done

for d in "${crawldirs[@]}" 
do
  numfiles=`find $d | wc -l`
  sizedir=`du -bs $d | tr "\t" ' ' | cut -d ' ' -f 1`
  echo " " $host $d $numfiles $sizedir
done


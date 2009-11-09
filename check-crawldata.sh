#!/bin/bash
#
# scan typical crawldirs on the localhost and report numfiles and
# sizedir for each crawldir. can be used via ssh to compare local
# crawldata to "staged" crawldata (crawldata copied from a crawler to
# a staging host) 
#
#  crawlname  name of crawl job, e.g. GEOCITIES-TEST
#  jobsdir    crawl job directory, e.g. /0/closure-jobs
#
#  numfiles   number of files in each crawldir as reported by "find"
#  sizedir    size of crawldir as reported by "du"
#
# see also:
#   check-crawldata-staged.sh
# examples:
#   /home/webcrawl/scripts/check-geo-staging.sh
#
# siznax 2009

if [ $# != 2 ]
then
  echo "Usage: $0 crawlname jobsdir"
  exit 1
fi

host=`hostname | cut -d '.' -f 1`
crawlname=$1
jobsdir=$2
jobdir=${jobsdir}/${crawlname}

if [ ! -d ${jobdir} ]
then
  echo "ERROR job not found:" ${jobdir}
  exit 2
fi

dir[0]=${jobdir}
dir[1]=/1/crawldata/${crawlname}
dir[2]=/2/crawldata/${crawlname}
dir[3]=/3/crawldata/${crawlname}
dir[4]=/0/incoming/${crawlname}
dir[5]=/1/incoming/${crawlname}
dir[6]=/2/incoming/${crawlname}
dir[7]=/3/incoming/${crawlname}

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


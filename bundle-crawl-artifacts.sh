#!/bin/bash
#
# make a tarball of crawl config, recover, reports, logs, and tombstones
# and leave in /var/tmp
#
#  crawlname  name of crawl job, e.g. GEOCITIES-TEST
#  jobsdir    crawl job directory, e.g. /0/closure-jobs
#  bundle     /var/tmp/{crawlname}-{host}-{mtime}-bundle.tar.gz
#
# see also:
#   make-and-store-bundle.sh
# examples:
#   /home/webcrawl/scripts/bundle-geo-crawls.sh
#
# brad 2008 (/home/webcrawl/tools/make-crl-bundle-general)
# siznax 2009

if [ $# != 2 ]
then
  echo "Usage: $0 crawlname jobsdir"
  exit 1
fi

echo $0 `date`

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

echo "checking searchdirs"
i=0
for d in "${dir[@]}"
do
  if [ -d $d ]
  then
    echo " " $d
    searchdirs[$i]=$d
    ((i++))
  fi
done

echo "changing wd: /var/tmp"
cd /var/tmp
tmpdir=/var/tmp/${crawlname}
logs=${tmpdir}/logs
recover=${tmpdir}/recover
reports=${tmpdir}/reports
config=${tmpdir}/config
warcs=${tmpdir}/warcs

if [ -d ${tmpdir} ]
then
  echo "ERROR tmpdir exists:" ${tmpdir} 
  exit 3
else
  echo "creating dir: ${tmpdir}"
  mkdir ${tmpdir}
  mkdir ${logs}
  mkdir ${reports}
  mkdir ${recover}
  mkdir ${config}
  mkdir ${warcs}
fi

echo "linking config files into ${config}"
find ${jobdir} -type f | grep -v scratch | grep -v maps | grep -v '.log$' | grep -v 'report.txt' | xargs -I % ln -s % ${config}

echo "linking logs into ${logs}"
find ${searchdirs[@]} -type f -name "*.log*" | xargs -I % ln -s % ${logs}

echo "linking recover files into ${recover}"
find ${searchdirs[@]} -type f -name "*.recover*" | xargs -I % ln -s % ${recover}

echo "linking report files into ${reports}"
find ${searchdirs[@]} -type f -name "*report.txt*" | xargs -I % ln -s % ${reports}

echo "linking warcs (tombstones) into ${warcs}"
find ${searchdirs[@]} -type f -name "*.tombstone" | xargs -I % ln -s % ${warcs}

mtime=`stat -c %y ${jobdir} | tr -d '-' | tr -d ':' | tr -d ' ' | cut -d '.' -f 1`
host=`hostname | cut -d '.' -f 1`
bundle=${crawlname}-${host}-${mtime}-bundle.tar.gz

echo "creating bundle: tar -chvf - ${crawlname} | gzip -9 /var/tmp/${bundle}"
tar -chvf - ${crawlname} | gzip -9 > $bundle 

echo "Done. " `date`

#!/bin/bash
#
# make a tarball of crawl config, recover, reports, logs, and tombstones
# and leave in /var/tmp
#
# INPUT
#
#  crawlname  name of crawl job
#  jobsdir    crawl job directory
#  jobroot    staged crawljob root
#  crawlhost  crawl host (for bundle name when crawldata is staged)
#
# OUTPUT
#
#  bundle  /var/tmp/{crawlname}-{host}-{mtime}-bundle.tar.gz
#            config
#            logs
#            recover
#            reports
#            warcs
#
# EXAMPLE
#
#  where /staged-crawljobs/STAGED_JOB/[0-3]
#        -> 0/closure-jobs/CRAWLNAME
#        -> 1/crawldata/CRAWLNAME
#        -> 2/crawldata/CRAWLNAME
#        -> 3/crawldata/CRAWLNAME
#        -> 1/incoming/CRAWLNAME
#        -> 3/incoming/CRAWLNAME
#
#  crawlname = CRAWLNAME
#  jobsdir   = /0/closure-jobs
#  jobroot   = /staged-crawljobs/STAGED_JOB
# 
# see also:
#   make-and-store-bundle.sh
# examples:
#   /home/webcrawl/scripts/bundle-geo-crawls.sh
#
# brad 2008 (/home/webcrawl/tools/make-crl-bundle-general)
# siznax 2009

if [ $# -lt 2 ]
then
  echo "Usage: $0 crawlname jobsdir [jobroot] [crawlhost]"
  exit 1
fi

echo $0 `date`

crawlname=$1
jobsdir=$2

if [ $3 ]
then
  jobroot=$3
else
  jobroot=''
fi

if [ $4 ]
then
  host=$4
else
  host=`hostname | cut -d '.' -f 1`
fi

jobdir=${jobroot}/${jobsdir}/${crawlname}
mtime=`stat -c %y ${jobdir}\
 | tr -d '-'\
 | tr -d ':'\
 | tr -d ' '\
 | cut -d '.' -f 1`
bundle=${crawlname}-${host}-${mtime}-bundle.tar.gz

if [ ! -d ${jobdir} ]
then
  echo "ERROR job not found:" ${jobdir}
  exit 2
else
  echo "crawlname: ${crawlname}"
  echo "jobdir   : ${jobdir}"
  echo "jobroot  : ${jobroot}"
fi

# potential searchdirs
dir[0]=${jobdir}                            # config
dir[1]=${jobroot}/1/crawldata/${crawlname}  # (logs, w/arcs)
dir[2]=${jobroot}/2/crawldata/${crawlname}  # (state)
dir[3]=${jobroot}/3/crawldata/${crawlname}  # (w/arcs)
dir[5]=${jobroot}/1/incoming/${crawlname}   # (tombstones)
dir[7]=${jobroot}/3/incoming/${crawlname}   # (tombstones)

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
find ${jobdir} -type f\
 | grep -v scratch\
 | grep -v maps\
 | grep -v '.log$'\
 | grep -v 'report.txt'\
 | grep -v '.divert$'\
 | grep -v '.schedule$'\
 | xargs -I % ln -s % ${config}

echo "linking logs into ${logs}"
find ${searchdirs[@]} -type f -name "*.log*"\
 | xargs -I % ln -s % ${logs}

echo "linking recover files into ${recover}"
find ${searchdirs[@]} -type f -name "*.recover*"\
 | xargs -I % ln -s % ${recover}

echo "linking report files into ${reports}"
find ${searchdirs[@]} -type f -name "*report.txt*"\
 | xargs -I % ln -s % ${reports}

echo "linking warcs (tombstones) into ${warcs}"
find ${searchdirs[@]} -type f -name "*.tombstone"\
 | xargs -I % ln -s % ${warcs}

echo "creating bundle: tar -chvf - ${crawlname} | gzip -9 /var/tmp/${bundle}"
tar -chvf - ${crawlname} | gzip -9 > ${bundle}

# cleanup
rm -rf ${tmpdir}

echo "Done. " `date`

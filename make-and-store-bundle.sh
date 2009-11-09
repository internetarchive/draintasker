#!/bin/bash
#
# envoke bundle-crawl-artifacts and store - use this to ssh into 
# a group of crawlers, bundle their jobs, and store them on a 
# staging server.
#
#  bundle     ${crawlname}-${host}-*-bundle.tar.gz
#  storehost  hostname of store server
#  storedir   storage dir on storehost
#  store      ${storehost}/${storedir}
#  log        /home/webcrawl/scripts/logs/${crawlname}-${host}-bundle.log
#
# see also:
#   bundle-crawl-artifacts.sh
# examples:
#   /home/webcrawl/scripts/bundle-geo-crawls.sh
#
# siznax 2009

if [ $# != 2 ]
then

  echo "Usage: $0 crawlname jobsdir storehost storedir"
  exit 1

else

  crawlname=$1
  jobsdir=$2
  host=`hostname | cut -d '.' -f 1`
  bundle=${crawlname}-${host}-*-bundle.tar.gz
  store="crawling08:/0/crawldata"
  log="/home/webcrawl/scripts/logs/${crawlname}-${host}-bundle.log"

  # echo "  cd /home/webcrawl/scripts/draintasker/dev/"
  cd /home/webcrawl/scripts/draintasker/dev/

  # echo "  ./bundle-crawl-artifacts.sh $crawlname $jobsdir > ../logs/${crawlname}-${host}-bundle.log"
  ./bundle-crawl-artifacts.sh $crawlname $jobsdir >| $log

  if [ $? -eq 0 ]
  then
    if [ -e /var/tmp/$bundle ]
    then
      scp /var/tmp/$bundle $store
    else
      echo "ERROR bundle not found:" $bundle
    fi
  else
    echo "ERROR bundling failed with status: $?"
    exit 2
  fi

fi

echo "Done $store/$bundle"

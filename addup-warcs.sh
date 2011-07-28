#!/bin/bash
# addup-warcs.sh [dir]
#
# find all arcs or warcs in dir (or current dir) add up 
# the size, and report w/arc_count and total size in bytes.
#
# siznax 2009

warc_dir=$1
total_size=0
warc_count=0

for w in `find $warc_dir/ \( -name \*.arc.gz -o -name \*.warc.gz \)`
do
  ((warc_count++))
  size=$(stat -c %s $w)
  total_size=$(( $total_size + $size ))
done

echo "found ($warc_count) warcs $total_size bytes"

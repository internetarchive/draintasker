#!/bin/bash 
#
# pack-warcs.sh job_dir xfer_dir max_size warc_naming [force] [mode]
#
# if DRAINME file exists, find <= max_size (in GB) of W/ARCs
# in job_dir, create warc_series_dir in xfer_dir, move series
# into xfer_dir, and leave PACKED file.
#
# if FINISH_DRAIN file found, then pack last warcs even 
# if << max_size
#
#   job_dir      /{0,1,2,3}/crawling/{crawljob}/warcs
#   xfer_dir     /{0,1,2,3}/incoming/{crawljob}
#   max_size     max size in GB of warcs to be transferred
#   warc_naming  integer for supported WARC naming (see drain.cfg)
#   [force]      1 = do not query user
#   [mode]       single = pack only 1 series and exit
#
# PREREQUISITES
#
#   DRAINME      /{job_dir}/DRAINME
#   FINISH_DRAIN /{job_dir}/FINISH_DRAIN
#                (optional, for finish draining)
#
# OUTPUT
#
#   warc_series  {prefix}-{timestamp}-{first}-{last}-{crawler}
#                prefix    w/arc file prefix
#                timestamp timestamp of first w/arc in series
#                first     serial number of first warc in series
#                last      serial number of last warc in series
#                crawler   crawl host from warc filename
#   PACKED       /{xfer_dir}/{warc_series}/PACKED
#
# siznax 2009

PG=$0; test -h $PG && PG=$(readlink $PG)
BIN=$(dirname $PG)

usage[0]="job_dir xfer_dir max_size warc_naming"
usage[1]="[force] [mode=single] [compactify=0]"

function unlock_job_dir {
  if [ -f $open ]; then
    echo "removing OPEN file: $open"
    rm $open || {
      echo "could not remove OPEN file: $open"
      exit 1
    }
  fi
}

function query_user {
  read -p "Continue [Y/n]> " text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

function compactify_target {
  # should be warc_naming_1 and 2 safe
  mv_prefix=`   echo $1 | cut -d '-' -f 1`
  mv_timestamp=`echo $1 | cut -d '-' -f 2`
  mv_serial=`   echo $1 | cut -d '-' -f 3`
  mv_ext=`      echo $1 | tr '.' ' ' | awk '{print $((NF-1))"."$NF}'`
  echo "$xfer_dir/${mv_prefix}-${mv_timestamp:0:14}-${mv_serial}.${mv_ext}"
  unset mv_prefix
  unset mv_timestamp
  unset mv_serial
}

# {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz
function set_warc_series_1 {
  b=`           echo $1 | cut -d '.' -f 1`
  prefix=`      echo $b | cut -d '-' -f 1`
  timestamp=`   echo $b | cut -d '-' -f 2`
  first_serial=`echo $b | cut -d '-' -f 3`
  crawler=`     echo $b | cut -d '-' -f 4`
  if (($compactify))
  then
      echo "${prefix}-${timestamp:0:14}-${crawler}"
  else
      echo "${prefix}-${timestamp}-${first_serial}"
  fi
}

# {TLA}-{timestamp}-{serial}-{PID}~{fqdn}~{port}.warc.gz
function set_warc_series_2 {
  b=`           echo $1 | cut -d '.' -f 1`
  prefix=`      echo $b | cut -d '-' -f 1`
  timestamp=`   echo $b | cut -d '-' -f 2`
  first_serial=`echo $b | cut -d '-' -f 3`
  crawler=`     echo $b | cut -d '~' -f 2`
  if (($compactify))
  then
      echo "${prefix}-${timestamp:0:14}-${crawler}"
  else
      echo "${prefix}-${timestamp}-${first_serial}"
  fi
}

function report_done {
    echo "$total_num_warcs warcs"\
         "$gz_OK_count gz_OK"\
         "$valid_count validated"\
         "$pack_count packed"\
         "$series_count series"
    echo `basename $0` "Done." `date`
}

################################################################

if (($# < 4)); then
  echo $(basename $0) ${usage[@]}
  exit 1
fi

job_dir=$1
xfer_home=$2
max_GB=$3  
warc_naming=$4
force=$5
mode=$6
compactify=$7

if [ -z $force ]; then force=0; fi
if [ -z $mode  ]; then mode=0; fi
if [ -z $compactify ]; then compactify=0; fi

echo `basename $0` `date`

if [ ! -d $job_dir ]
then
  echo "ERROR: job_dir not found: $job_dir"
  exit 1
fi
if [ $warc_naming = 1 ]; then
  set_warc_series=set_warc_series_1
else
  set_warc_series=set_warc_series_2
fi

std_warc_size=$(( 1024 * 1024 * 1024 )) # 1 gibibyte
max_size=$(( $max_GB * $std_warc_size ))
warc_series=''
warcs_per_series=$(( $max_size / $std_warc_size ))

# check for warcvalidator on path ($WARC_TOOLS/app/warcvalidator)
# 
# WARCVALIDATOR DISABLED
# 
# warcvalidator="$WARC_TOOLS/app/warcvalidator"
# if [ ! -e "$warcvalidator" ]
# then
#   echo "ERROR: warcvalidator not found: $warcvalidator"
#   exit 2
# fi

open="$job_dir/PACKED.open"
DRAINME="$job_dir/DRAINME"
FINISH_DRAIN="$job_dir/FINISH_DRAIN"
total_num_warcs=`ls $job_dir/*.{arc,warc}.gz 2>/dev/null | wc -l`
total_size_warcs=`$BIN/addup-warcs.sh $job_dir | awk '{print $4}'`
est_num_series=$(( $total_num_warcs / $warcs_per_series )) 

echo "  DRAINME          = $DRAINME"
echo "  job_dir          = $job_dir"
echo "  xfer_home        = $xfer_home"
echo "  warc_naming      = $warc_naming"
echo "  std_warc_size    = $std_warc_size (1GB)"
echo "  max_series_size  = $max_size (${max_GB}GB)"
echo "  warcs_per_series = $warcs_per_series"
echo "  total_num_warcs  = $total_num_warcs"
echo "  total_size_warcs = $total_size_warcs"
echo "  est_num_series   = $est_num_series (estimated)"
echo "  FINISH_DRAIN     = $FINISH_DRAIN"
echo "  OPEN             = $open"
#echo "  PACKED           = $PACKED"
echo "  mode             = $mode"
echo "  compactify       = $compactify"

if [ $force -ne 1 ]; then query_user; fi

# look for DRAINME
if [ ! -e $DRAINME ]; then
  echo "DRAINME file not found, exiting."
  exit 0
fi

# abort packing when less than 10 warcs and no FINISH_DRAIN
if [ ! -f $FINISH_DRAIN ]; then
  if [ $total_num_warcs -lt $max_GB ]; then
    echo "too few WARCs and FINISH_DRAIN file not found, exiting normally"
    exit 0
  fi
fi

# lock this process
if [ -e $open ]; then
  echo "OPEN file exists: $open"
  exit 0
else
  trap unlock_job_dir EXIT
  echo "creating file: $open"
  touch $open || {
    echo "could not touch OPEN file: $open"
    exit 1
  }
fi

warc_count=0
series_count=0
pack_count=0
valid_count=0
gz_OK_count=0
msize=0    # manifest size
mfiles=()  # manifest files array

# loop over warcs in job dir
cd $job_dir
for w in `find $job_dir\
    -maxdepth 1 \( -name \*.arc.gz -o -name \*.warc.gz \)\
    | sort`
do 
  # check gzip container
  echo "  verifying gz: $(basename $w)"
  zcat $w > /dev/null || {
    echo "ERROR: bad gzip, skipping file: $w"
    echo "  mv $w $w.bad"
    mv $w "${w}.bad"
    continue
  }
  ((gz_OK_count++))

  # validate WARC - TBD
  # echo "  validating WARC: $w"
  # $warcvalidator -f $w
  # if [ $? != 0 ]; then
  #   err="ERROR: invalid warc: ${mfiles[${i}]} $?"
  #   echo "  mv $w ${w}.invalid"
  #   mv $w "${w}.invalid"
  #   continue
  # fi
  # ((valid_count++))

  # increment msize
  fsize=$(stat -c %s $w)
  ((msize+=$fsize))

  ((warc_count++))
  # "1" if w is the last warc in job_dir
  is_last_warc=$(($warc_count == $total_num_warcs))

  # keep adding file until msize > max_size, or the last file
  if (($msize <= $max_size && !$is_last_warc)); then
      mfiles+=("$w")
      continue
  fi

  # only pack last warcs if FINISH_DRAIN
  if (($is_last_warc)); then
    mfiles+=("$w")
    next_mfiles=()
    next_msize=0
    if [ -e $FINISH_DRAIN ]; then
      echo "FINISH_DRAIN file found, packing last warcs"\
	   "(${#mfiles[@]})"
    else
      echo "FINISH_DRAIN file not found, leaving last warcs"\
	   "(${#mfiles[@]})"
      continue
    fi
  elif ((${#mfiles[0]} == 0)); then
    # first file is larger than $max_size - pack it by itself
    mfiles+=("$w")
    next_mfiles=()
    next_msize=0
  else
    # regular case - send $w to next item
    ((msize-=$fsize))
    next_mfiles=("$w")
    next_msize=$fsize
  fi

  ((series_count++))

  warc_series=$($set_warc_series $(basename "${mfiles[0]}"))

  last_serial=`echo "${mfiles[${#mfiles[@]}-1]}"\
      | awk '{l=split($job_dir, a, "-"); printf(a[l-1]);}'`
  # FIXME this results in two crawler name in warc_series
  if ((!$compactify)); then
      warc_series="${warc_series}-${last_serial}-${crawler}"
  fi
  pack_info="$warc_series ${#mfiles[@]} $msize"

  xfer_dir="$xfer_home/${warc_series}"

  echo "files considered for packing:" 
  for ((i=0; i<${#mfiles[@]}; i++)); do
      printf "%5s %s\n" [$i] ${mfiles[$i]}
  done

  echo
  echo "WARNING: WARC naming may have changed, check series: $warc_series"
  echo

  echo "==== $pack_info  ====  "

  # make xfer_home
  if [ -d $xfer_home ]; then
    echo "$xfer_home exists"
  else
    echo "mkdir $xfer_home"
    mkdir $xfer_home
    if [ $? != 0 ]; then
	echo "ERROR: mkdir failed: $xfer_home"
	exit 1
    fi
  fi

  # make xfer_dir
  if [ -d $xfer_dir ]; then
    echo "$xfer_dir exists"
    if [ -f $xfer_dir/PACKED ]; then
      echo $xfer_dir/PACKED exists - item name conflict, aborting
      exit 1
    fi
  else
    echo "mkdir $xfer_dir"
    mkdir $xfer_dir
    if [ $? != 0 ]; then
	echo "ERROR: mkdir failed: $xfer_dir"
	exit 1
    fi
  fi

  # move files in this manifest
  for ((i=0;i<${#mfiles[@]};i++))
  do
    if (($compactify)); then
	source=${mfiles[$i]}
	target=$(compactify_target $(basename ${mfiles[$i]}))
    else
	source=${mfiles[$i]}
	target=$xfer_dir
    fi
    echo "mv $source $target"
    mv $source $target || {
      echo "ERROR: mv failed"
      exit 1
    }
    ((pack_count++))  
    unset source
    unset target
  done       

  # leave PACKED file
  echo "echo '$pack_info' > $xfer_dir/PACKED"
  echo $pack_info > $xfer_dir/PACKED

  # check mode
  if [ $mode == 'single' ]; then
    echo "mode = $mode, exiting normally."
    break
  fi

  # start next warc_series
  echo " "

  # reset item/manifest
  msize=$next_msize
  mfiles=("${next_mfiles[@]}")
  last_serial=''
  xfer_dir=''
done

# this unlock_job_dir will be executed at script exit anyway, but
# it is nice "removing OPEN file" message appears before Done message.
unlock_job_dir
report_done
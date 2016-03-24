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
# NB: existence of DRAINME is no longer a prerequisite. it is now checked
# by dtmon.py (i.e. it now controls "automatic" draining only.)
#
# siznax 2009

PG=$0; test -h $PG && PG=$(readlink $PG)
BIN=$(dirname $PG)

usage="config [force] [mode=single]"

function unlock_job_dir {
  if [ -f $open ]; then
    echo "removing $open"
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

# parse_warc_name WARC-NAME VAR_PREFIX
# decompose WARC-NAME according to WARC_NAME_PATTERN and stores
# each component into variable ${VAR_PREFIX}COMPONENT
function parse_warc_name {
  local re=$(sed -e 's/{[^}]*}/(.*)/g' <<<"$WARC_NAME_PATTERN")
  local names=($(sed -e 's/[^}]*{\([^}]*\)}[^{]*/\1 /g' <<<"$WARC_NAME_PATTERN") ext gz)

  if [[ "$1" =~ ^$re(\.w?arc(\.gz)?)$ ]]; then
    read ${names[@]/#/$2} <<<"${BASH_REMATCH[@]:1}"
    # host name without domain part
    eval ${2}shost='$( cut -d . -f 1 <<<"$'$2'host" )'
  else
    return 1
  fi
}

function compactify_target {
  parse_warc_name "$1" mv_
  echo "${mv_prefix}-${mv_timestamp:0:14}-${mv_serial}${mv_ext}"
}

function make_item_name {
  parse_warc_name "$1" ''
  parse_warc_name "$2" last
  #local prefix=${f_prefix} host=${f_host} shost=${f_shost}
  #local timestamp=${f_timestamp} timestamp14=${f_timestamp:0:14}
  local timestamp14=${timestamp:0:14} lasttimestamp14=${lasttimestamp:0:14}
  #local serial=${f_serial} lastserial=${l_serial}
  eval echo $ITEM_NAME_TEMPLATE
}

function report_done {
    echo "$total_num_warcs warcs,"\
         "$gz_OK_count gz_OK,"\
         "$valid_count validated,"\
         "$pack_count packed,"\
         "$series_count series"
    echo $(basename $0) done. $(date)
}

################################################################

#if (($# < 4)); then
if (($# < 1)); then
  echo $(basename $0) ${usage[@]}
  exit 1
fi

echo $(basename $0) $(date)

CONFIG=$1
$BIN/config.py $CONFIG || {
  echo "ERROR: invalid config: $CONFIG"
  exit 1
}
force=${2:-0}
mode=${3:-single}
# get config parameters
job_dir=$($BIN/config.py $CONFIG job_dir)
xfer_home=$($BIN/config.py $CONFIG xfer_dir)
max_GB=$($BIN/config.py $CONFIG max_size)
compactify=$($BIN/config.py $CONFIG compact_names)
WARC_NAME_PATTERN="$($BIN/config.py $CONFIG warc_name_pattern)"
item_naming="$($BIN/config.py $CONFIG item_name_template)"
ITEM_NAME_TEMPLATE="$($BIN/config.py $CONFIG item_name_template_sh)"
verify_gzip=$($BIN/config.py $CONFIG verify_gzip)
SUFFIX_RE=$($BIN/config.py $CONFIG suffix_re)
if [ -z "$SUFFIX_RE" ]; then
    SUFFIX_RE='\.w?arc\(\.gz\)?'
fi
if [ ! -d $job_dir ]; then
  echo "ERROR: job_dir not found: $job_dir"
  exit 1
fi

max_size=$(( max_GB * 1024 * 1024 * 1024 ))
warc_series=''

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

WARC_NAME_RE="$(sed -e 's/{[^}]*}/\\(.*\\)/g' <<<"$WARC_NAME_PATTERN")"
WARC_NAME_RE_FIND=".*/${WARC_NAME_RE}${SUFFIX_RE}"'$'

open="$job_dir/PACKED.open"
FINISH_DRAIN="$job_dir/FINISH_DRAIN"
total_num_warcs=0
total_size_warcs=0

# lock this process
if [ -e $open ]; then
  pid=$(head -1 $open)
  if [[ $pid =~ ^[0-9]+$ ]]; then
    if ps -p $pid >/dev/null 2>&1; then
      echo "OPEN file exists: $open (PID=$pid)"
      echo $(basename $0) done. $(date)
      exit 0
    else
      echo "Removing stale $open (PID=$pid)"
    fi
  else
    echo "OPEN file exists: $open (PID unknown)"
    echo $(basename $0) done. $(date)
    exit 0
  fi
fi
trap unlock_job_dir EXIT
echo "creating file: $open"
echo $$ > $open || {
  echo "could not touch OPEN file: $open"
  exit 1
}

for w in $(find $job_dir -maxdepth 1 -regex "${WARC_NAME_RE_FIND}"); do
    ((total_num_warcs++))
    ((total_size_warcs += $(stat -c %s $w)))
done

echo "  job_dir          = $job_dir"
echo "  xfer_home        = $xfer_home"
echo "  warc_naming      = $WARC_NAME_PATTERN"
echo "  item_naming      = $item_naming"
echo "  max_series_size  = $max_size (${max_GB}GB)"
echo "  total_num_warcs  = $total_num_warcs"
echo "  total_size_warcs = $total_size_warcs"
echo "  FINISH_DRAIN     = $FINISH_DRAIN"
echo "  OPEN             = $open"
echo "  mode             = $mode"
echo "  compactify       = $compactify"

if [ "$force" -ne 1 ]; then query_user; fi

# abort packing when less than max_GB warcs and no FINISH_DRAIN
if [ ! -f $FINISH_DRAIN ]; then
  if ((total_size_warcs < max_size)); then
    echo $(basename $0) "too few WARCs and FINISH_DRAIN file not found, exiting normally"
    exit 0
  fi
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
for w in $(find $job_dir -maxdepth 1 -regex "${WARC_NAME_RE_FIND}" | sort)
do 
  if [[ $w =~ \.gz$ ]]; then
    # check gzip container
    if [ "$mode" != test -a "$verify_gzip" != 0 ]; then
      echo "  verifying gz: $(basename $w)"
      gzip -t $w > /dev/null || {
	echo "ERROR: bad gzip, skipping file: $w"
	echo "  mv $w $w.bad"
	mv $w "${w}.bad"
	continue
      }
    fi
    ((gz_OK_count++))
  fi

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
  ((msize+=fsize))

  ((warc_count++))
  # "1" if w is the last warc in job_dir
  is_last_warc=$((warc_count == total_num_warcs))

  # keep adding file until msize > max_size, or the last file
  if ((msize <= max_size && !is_last_warc)); then
      mfiles+=("$w")
      continue
  fi

  # only pack last warcs if FINISH_DRAIN
  if ((is_last_warc)); then
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
  elif ((${#mfiles[@]} == 0)); then
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

  suffix=''
  # breaks when item directory is secured successfully
  while true; do
    warc_series=$(make_item_name $(basename "${mfiles[0]}") \
      $(basename "${mfiles[${#mfiles[@]}-1]}"))
    if [ -z $warc_series ]; then
      echo "ERROR:item identifier generation failed"
      exit 1
    fi

    pack_info="$warc_series ${#mfiles[@]} $msize"

    echo "files considered for packing:" 
    i=0
    for file in ${mfiles[@]}; do
	printf "%5s %s\n" [$((++i))] $file
    done

    echo "==== $pack_info ===="

    # make xfer_dir
    xfer_dir="$xfer_home/${warc_series}"
    if [ -d $xfer_dir ]; then
      echo "$xfer_dir exists"
      if [ -f $xfer_dir/PACKED ]; then
	if ((compactify)); then
	  echo $xfer_dir/PACKED exists - item name conflict, adding suffix to resolve
	  ((--suffix)) # sets "-1" to suffix when suffix==''
	else
	  echo $xfer_dir/PACKED exists - item name conflict, aborting
	  exit 1
	fi
      fi
    else
      echo "mkdir -p $xfer_dir"
      mkdir -p $xfer_dir || {
	echo "ERROR: mkdir failed: $xfer_dir"
	exit 1
      }
      break
    fi
  done

  # move files in this manifest
  for source in ${mfiles[@]}; do
    if ((compactify)); then
      target=$xfer_dir/$(compactify_target $(basename $source))
    else
      target=$xfer_dir
    fi
    echo "mv $source $target"
    if [ "$mode" != test ]; then
	mv $source $target || {
	  echo "ERROR: mv failed"; exit 1
	}
    fi
    ((pack_count++))
    unset source
    unset target
  done       

  # leave PACKED file
  echo "PACKED: $pack_info"
  echo $pack_info > $xfer_dir/PACKED

  # check mode
  if [ "$mode" == single -o "$mode" == test ]; then
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

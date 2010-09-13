#!/bin/bash 
# pack-warcs.sh job_dir xfer_dir max_size [force] [mode]
#
# if DRAINME file exists, find <= max_size (in GB) of W/ARCs
# in job_dir, create warc_series_dir in xfer_dir, move series
# into xfer_dir, and leave PACKED file.
#
# if FINISH_DRAIN file found, then pack last warcs even 
# if << max_size
#
#   job_dir      /{crawldata}/{JOB}
#   xfer_dir     /{rsync_path}/{JOB}
#   max_size     max size in GB of warcs to be transferred
#   [force]      do not query user
#   [mode]       single = pack only 1 series and exit
#
#   CONFIG       ./dtmon.cfg
#   DRAINME      /{crawldata}/{JOB}/DRAINME
#   warc_series  {prefix}-{timestamp}-{first}-{last}-{crawler}
#                prefix    w/arc file prefix
#                timestamp timestamp of first w/arc in series
#                first     serial number of first warc in series
#                last      serial number of last warc in series
#                crawler   crawl host from warc filename
#   PACKED       /{xfer_dir}/{warc_series}/PACKED
#   FINISH_DRAIN /{crawldata}/{JOB}/FINISH_DRAIN
#
# siznax 2009

usage="$0 job_dir xfer_dir max_size [force] [mode=single]"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

function set_warc_series_1 {
  # naming scheme change broke this!
  # https://webarchive.jira.com/browse/HER-1727
  #
  # WAS {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz
  # IS  {TLA}-{timestamp}-{serial}-{PID}~{fqdn}~{port}.warc.gz

  b=`echo $f | cut -d '.' -f 1 | grep -o "[^/]*" | tail -1`
  crawler=`echo $b | cut -d '~' -f 2`
  first_serial=`echo $b | tr "-" " " | awk '{print $((NF-1))}'`
  timestamp=`echo $b | tr "-" " " | awk '{print $((NF-2))}'`
  prefix=`echo $b | sed "s/-${timestamp}-${first_serial}-${crawler}//"`
  warc_series="${prefix}-${timestamp}-${first_serial}"
}

function set_warc_series_2 {
  b=`echo $f | cut -d '.' -f 1`
  prefix=`echo $b | cut -d '-' -f 1`
  timestamp=`echo $b | cut -d '-' -f 2`
  first_serial=`echo $b | cut -d '-' -f 3`
  crawler=`echo $b | cut -d '~' -f 2`
  warc_series="${prefix}-${timestamp}-${first_serial}"
}

################################################################

if [ -n "$3" ]
then

  if [ -n "$5" ]; then mode=$5; else mode=0; fi

  echo $0 `date`

  job_dir=$1

  if [ ! -d $job_dir ]
  then
    echo "ERROR: job_dir not found: $job_dir"
    exit 1
  fi

  std_warc_size=$(( 1024 * 1024 * 1024 )) # 1 gibibyte
  max_size=$(( $3 * $std_warc_size ))
  warc_series=''
  warcs_per_series=$(( $max_size / $std_warc_size ))

  job_name=`echo $job_dir | tr '/' " " | awk '{print $NF}'`
  xfer_home=$2

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
  CONFIG="${PWD}/dtmon.cfg"
  PACKED="$job_dir/PACKED"
  ERROR="$job_dir/ERROR"
  DRAINME="$job_dir/DRAINME"
  FINISH_DRAIN="$job_dir/FINISH_DRAIN"
  total_num_warcs=`ls $job_dir/*.{arc,warc}.gz | wc -l`
  total_size_warcs=`./addup-warcs.sh $job_dir | awk '{print $4}'`
  est_num_series=$(( $total_num_warcs / $warcs_per_series )) 
  warc_naming=`grep ^WARC_naming $CONFIG | tr -s ' '\
    | cut -d ' ' -f 2 | tail -1`

  if [ -z $warc_naming ]
  then 
    echo "ERROR: must select warc_naming scheme in CONFIG: $CONFIG"
    exit 1
  fi

  echo "  DRAINME          = $DRAINME"
  echo "  job_dir          = $job_dir"
  echo "  xfer_home        = $xfer_home"
  echo "  job_name         = $job_name"
  echo "  warc_naming      = $warc_naming"
  echo "  max_series_size  = ${3}GB ($max_size)"
  echo "  std_warc_size    = $std_warc_size (1GB)"
  echo "  warcs_per_series = $warcs_per_series"
  echo "  total_num_warcs  = $total_num_warcs"
  echo "  total_size_warcs = $total_size_warcs"
  echo "  est_num_series   = $est_num_series (estimated)"
  echo "  FINISH_DRAIN     = $FINISH_DRAIN"
  echo "  OPEN             = $open"
  echo "  PACKED           = $PACKED"
  echo "  mode             = $mode"

  if [ -z "$4" ]
  then
    query_user
  fi

  # look for DRAINME
  if [ ! -e $DRAINME ]
  then
    echo "DRAINME file not found, exiting."
    exit 0
  fi

  # lock this process
  if [ -e $open ]; then
    echo "OPEN file exists: $open"
    exit 0
  else
    echo "opening file: $open"
    touch $open
    if [ $? != 0 ]; then
      echo "could not touch OPEN file: $open"
      exit 1
    fi
  fi

  warc_count=0
  series_count=0
  pack_count=0
  valid_count=0
  msize=0    # manifest size
  mcount=0   # manifest file count
  mfiles=()  # manifest files array

  # loop over warcs in job dir
  back=`pwd`
  cd $job_dir
  for w in `find $job_dir -maxdepth 1 \( -name \*.arc.gz -o -name \*.warc.gz \) | sort`
  do 

    # increment msize
    f=`echo $w | tr "/" " " | awk '{print $NF}'`
    fsize=`ls -l $f | nawk '{print $5}'`
    msize=$(( $msize + $fsize ))
    mfiles[$mcount]=$w

    # begin warc_series
    if [ -z $warc_series ]
    then
      if [ $warc_naming == 1 ]
      then
        set_warc_series_1
      else
        set_warc_series_2
      fi
    fi

    # last warc
    if [ $warc_count -eq $(( $total_num_warcs - 1 )) ] 
    then
      is_last_warc='true'
    fi

    # new item/manifest when msize > max_size
    if [ $msize -gt $max_size ] || [ "$is_last_warc" == 'true' ]
    then 

      # only pack last warcs if FINISH_DRAIN
      if [ "$is_last_warc" == 'true' ] 
      then
        last_warc_count=$(($mcount + 1))
        echo "mfiles[${mcount}] ${mfiles[${mcount}]}"\
             "$warc_series $fsize $msize"
        if [ -e $FINISH_DRAIN ]
        then
          echo "FINISH_DRAIN file found, packing last warcs"\
               "($last_warc_count)"
        else
          echo "FINISH_DRAIN file not found, leaving last warcs"\
               "($last_warc_count)"
	  continue
        fi
      fi

      ((series_count++))

      # finish warc series
      if [ "$is_last_warc" == 'true' ]
      then
        last_serial=`echo "${mfiles[${mcount}]}"\
	    | awk '{l=split($job_dir, a, "-"); printf(a[l-1]);}'`
        num_mfiles=$(( ${#mfiles[@]} ))
        (( mcount++ ))
        warc_series="${warc_series}-${last_serial}-${crawler}"
        pack_info="$warc_series $mcount $msize"
      else
        last_serial=`echo "${mfiles[${mcount}-1]}"\
            | awk '{l=split($job_dir, a, "-"); printf(a[l-1]);}'`
        num_mfiles=$(( ${#mfiles[@]} - 1 ))
        warc_series="${warc_series}-${last_serial}-${crawler}"
        pack_info="$warc_series $mcount $prev_msize"
      fi 
      xfer_dir="$xfer_home/${warc_series}"

echo
echo "WARNING: WARC naming may have changed, check series: $warc_series"

      echo " "
      echo "==== $pack_info  ====  "
      echo " "

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
      else
        echo "mkdir $xfer_dir"
	mkdir $xfer_dir
	if [ $? != 0 ]; then
	    echo "ERROR: mkdir failed: $xfer_dir"
	    exit 1
	fi
      fi

      # validate files in this manifest
      for (( i=0;i<$num_mfiles;i++)); do
	echo "VALIDATION DISABLED ${mfiles[${i}]}"
        # echo "validating ${mfiles[${i}]}"
        # $warcvalidator -f ${mfiles[${i}]}
	# if [ $? != 0 ]; then
	#   err="ERROR: warc validation failed: ${mfiles[${i}]} $?"
	#   echo $err > $ERROR
	#   exit 2
	# else
	#   ((valid_count++))  
	# fi
      done       

      # move files in this manifest
      for (( i=0;i<$num_mfiles;i++)); do
        echo "mv ${mfiles[${i}]} $xfer_dir"
        mv ${mfiles[${i}]} $xfer_dir
	if [ $? != 0 ]; then
	  echo "ERROR: mv failed"
	  exit 1
	else
	  ((pack_count++))  
	fi
      done       

      # leave PACKED file
      echo "echo '$pack_info' > $xfer_dir/PACKED"
      echo $pack_info > $xfer_dir/PACKED

      # check mode
      if [ $mode == 'single' ]
      then
        echo "$total_num_warcs warcs $pack_count packed $series_count series"
        echo "mode = $mode, exiting normally."
	echo "removing OPEN file: $open"
        rm $open  # unlock this process
        exit 0
      fi

      # start next warc_series
      echo " "
      if [ $warc_naming == 1 ]
      then
        set_warc_series_1
      else
        set_warc_series_2
      fi

      # reset item/manifest
      msize=$fsize
      mcount=0
      mfiles=()
      mfiles[$mcount]=$w
      last_serial=''
      xfer_dir=''

    fi

    if [ "$is_last_warc" != 'true' ]
    then
      echo "mfiles[${mcount}] ${mfiles[${mcount}]}"\
           "$warc_series $fsize $msize"
    fi

    ((mcount++))
    prev_msize=$msize
    ((warc_count++))

  done

  # unlock this process
  echo "rm OPEN file: $open"
  rm $open
  if [ $? != 0 ]; then
    echo "could not close (rm) OPEN file: $open"
    exit 1
  fi

  cd $back

else 
  echo $usage
  exit 1
fi

echo "$total_num_warcs warcs $valid_count validated $pack_count"\
     "packed $series_count series"
echo "$0 Done." `date`

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

usage[0]="job_dir xfer_dir max_size warc_naming"
usage[1]="[force] [mode=single] [compactify=0]"

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

function compactify_target {
  # should be warc_naming_1 and 2 safe
  mv_prefix=`   echo $mfile | cut -d '-' -f 1`
  mv_timestamp=`echo $mfile | cut -d '-' -f 2`
  mv_serial=`   echo $mfile | cut -d '-' -f 3`
  mv_ext=`      echo $mfile | tr '.' ' ' | awk '{print $((NF-1))"."$NF}'`
  target="$xfer_dir/${mv_prefix}-${mv_timestamp:0:14}-${mv_serial}.${mv_ext}"
  unset mv_prefix
  unset mv_timestamp
  unset mv_serial
}

# {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz
function set_warc_series_1 {
  b=`           echo $f | cut -d '.' -f 1`
  prefix=`      echo $b | cut -d '-' -f 1`
  timestamp=`   echo $b | cut -d '-' -f 2`
  first_serial=`echo $b | cut -d '-' -f 3`
  crawler=`     echo $b | cut -d '-' -f 4`
  if [ $compactify -eq 1 ]
  then
      warc_series="${prefix}-${timestamp:0:14}-${crawler}"
  else
      warc_series="${prefix}-${timestamp}-${first_serial}"
  fi
}

# {TLA}-{timestamp}-{serial}-{PID}~{fqdn}~{port}.warc.gz
function set_warc_series_2 {
  b=`           echo $f | cut -d '.' -f 1`
  prefix=`      echo $b | cut -d '-' -f 1`
  timestamp=`   echo $b | cut -d '-' -f 2`
  first_serial=`echo $b | cut -d '-' -f 3`
  crawler=`     echo $b | cut -d '~' -f 2`
  if [ $compactify -eq 1 ]
  then
      warc_series="${prefix}-${timestamp:0:14}-${crawler}"
  else
      warc_series="${prefix}-${timestamp}-${first_serial}"
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

if [ $# -gt 4 ]
then

  job_dir=$1
  xfer_home=$2
  max_GB=$3  
  warc_naming=$4
  force=$5
  mode=$6
  compactify=$7

  if [ -z $force ]; then force=0; fi
  if [ -z $mode  ]; then mode=0;  fi
  if [ -z $compactify ]; then compactify=0;  fi

  echo `basename $0` `date`

  if [ ! -d $job_dir ]
  then
    echo "ERROR: job_dir not found: $job_dir"
    exit 1
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
  PACKED="$job_dir/PACKED"
  ERROR="$job_dir/ERROR"
  DRAINME="$job_dir/DRAINME"
  FINISH_DRAIN="$job_dir/FINISH_DRAIN"
  total_num_warcs=`ls $job_dir/*.{arc,warc}.gz 2>/dev/null | wc -l`
  total_size_warcs=`./addup-warcs.sh $job_dir | awk '{print $4}'`
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
  echo "  PACKED           = $PACKED"
  echo "  mode             = $mode"

  if [ $force -ne 1 ]; then query_user; fi

  # look for DRAINME
  if [ ! -e $DRAINME ]
  then
    echo "DRAINME file not found, exiting."
    exit 0
  fi

  # abort packing when less than 10 warcs and no FINISH_DRAIN
  if [ ! -f $FINISH_DRAIN ]
  then
      if [ $total_num_warcs -lt $max_GB ]
      then
          echo "too few WARCs and FINISH_DRAIN file not found, exiting normally"
          exit 0
      fi
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
  gz_OK_count=0
  msize=0    # manifest size
  mcount=0   # manifest file count
  mfiles=()  # manifest files array

  # loop over warcs in job dir
  back=`pwd`
  cd $job_dir
  for w in `find $job_dir\
      -maxdepth 1 \( -name \*.arc.gz -o -name \*.warc.gz \)\
      | sort`
  do 

    # check gzip container
    echo "  verifying gz: $w"
    zcat $w > /dev/null
    if [ $? != 0 ]
    then
        echo "ERROR: bad gzip, skipping file: $w"
        echo "  mv $w $w.bad"
        mv $w "${w}.bad"
        continue
    fi
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
        if [ $compactify -ne 1 ]
        then
            warc_series="${warc_series}-${last_serial}-${crawler}"
        fi
        pack_info="$warc_series $mcount $msize"
      else
        last_serial=`echo "${mfiles[${mcount}-1]}"\
            | awk '{l=split($job_dir, a, "-"); printf(a[l-1]);}'`
        num_mfiles=$(( ${#mfiles[@]} - 1 ))
        if [ $compactify -ne 1 ]
        then
            warc_series="${warc_series}-${last_serial}-${crawler}"
        fi
        pack_info="$warc_series $mcount $prev_msize"
      fi 
      xfer_dir="$xfer_home/${warc_series}"

      echo "files considered for packing:" 
      count=0
      for ((i=0; i<${#mfiles[@]}; i++))
      do
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
      else
        echo "mkdir $xfer_dir"
	mkdir $xfer_dir
	if [ $? != 0 ]; then
	    echo "ERROR: mkdir failed: $xfer_dir"
	    exit 1
	fi
      fi

      # move files in this manifest
      for (( i=0;i<$num_mfiles;i++))
      do
        mfile=`echo ${mfiles[${i}]} | tr '/' ' ' | awk '{print $NF}'`
        if [ $compactify -eq 1 ]
        then
            source=${mfiles[${i}]}
            compactify_target
        else
            source=${mfiles[${i}]}
            target=$xfer_dir
        fi
        echo "mv $source $target"
        mv $source $target
	if [ $? != 0 ]
        then
	  echo "ERROR: mv failed"
	  exit 1
	else
	  ((pack_count++))  
          unset source
          unset target
	fi
      done       

      # leave PACKED file
      echo "echo '$pack_info' > $xfer_dir/PACKED"
      echo $pack_info > $xfer_dir/PACKED

      # check mode
      if [ $mode == 'single' ]
      then
	echo "removing OPEN file: $open"
        rm $open  # unlock this process
        echo "mode = $mode, exiting normally."
        report_done
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
  echo `basename $0` ${usage[@]}
  exit 1
fi

report_done
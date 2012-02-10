#!/bin/bash
#
# s3-launch-transfers.sh config [force] [mode]
#
# foreach warc_series in xfer_job_dir, submit an S3 HTTP request
# to upload files to remote storage - unless one of the 
# following files exists: LAUNCH, LAUNCH.open, ERROR, TASK
#
#  config   config params in this file
#  [force]  optionally skip interactive continue
#  [mode]   single = submit 1 task, then exit
#
# CONFIGURATION
#
#   config
#     xfer_dir
#     title_prefix
#     collection_prefix
#     test_suffix
#     block_delay
#     max_block_count
#     retry_delay
#
#   $HOME/.ias3cfg
#     access_key
#     secret_key
#
# INPUT
#
#   MANIFEST  {warc_series}/MANIFEST
#
# OUTPUT
#
#   LAUNCH     {warc_series}/LAUNCH
#   TASK       {warc_series}/TASK
#   SUCCESS    {warc_series}/SUCCESS
#   TOMBSTONE  {warc_series}/TOMBSTONE
#
# siznax 2010

PG=$0; test -h $PG && PG=$(readlink $PG)
BIN=$(dirname $PG)

usage="config [force] [mode=single]"

: ${TEST:=false}
if $TEST; then
    CURL=curl_fake
else
    CURL=curl
fi
s3='s3.us.archive.org'
dl='www.archive.org/download'
std_warc_size=$(( 1024*1024*1024 )) # 1 gibibyte

################################################################

function query_user {
  read -p 'Continue [Y/n]> ' text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}
function echorun {
    echo "$*"
    "$@"
}

function parse_warc_name {
  local re=$(sed -e 's/{[^}]*}/(.*)/g' <<<"$WARC_NAME_PATTERN")
  local names=($(sed -e 's/[^}]*{\([^}]*\)}[^{]*/\1 /g' <<<"$WARC_NAME_PATTERN") ext gz)

  if [[ "$1" =~ ^$re(\.w?arc(\.gz)?)$ ]]; then
    read ${names[@]/#/$2} <<<"${BASH_REMATCH[@]:1}"
  else
    return 1
  fi
}

function set_tHR { # human-readable date format
    local t=$1
    date -d "${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}"
}

# ISO 8601 date format
function set_tISO {
    local t=$1
    date +%Y-%m-%dT%T%Z -d "${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}"
}

# we could do something more sophisticated here
# for more human readable date ranges
function set_date_range {
    local t=$(parse_warc_name "$(basename "$1")"; echo $timestamp)
    first_file_date="$t"
    start_date_ISO="$(set_tISO "$t")"
    start_date_HR="$(set_tHR "$t")"

    # # get warc start date
    # first_file=`echo ${files[0]} | tr '/' ' ' | awk '{print $NF}'`
    # if [ $compact_names == 1 ]
    # then
    #     t=`echo $files[0] | cut -d '-' -f 2`
    # else
    #     t=`echo $files[0] | cut -d '-' -f 2 | grep -o "[0-9]\{14,17\}"`000
    # 	t=${t:0:17}
    # fi
    # first_file_date=$t
    # set_tISO
    # start_date_ISO=${tISO}
    # set_tHR
    # start_date_HR=${tHR}

    scandate=${t:0:14}
    metadate=${t:0:4}
    
    # get dates from last file in series
    last_file_date=$(parse_warc_name $(basename "$2"); echo $timestamp)

    # warc end date (from last file mtime). should (closely) 
    # correspond to time of last record in series
    local t=$(date +%Y%m%d%H%M%S -d @"$(stat -c %Y "$2")")
    last_date=$t
    end_date_ISO="$(set_tISO "$t")"
    end_date_HR="$(set_tHR "$t")"
    
    # if [ ${#files[@]} -gt 1 ]; then
    # 	last_file=${files[${#files[@]}-1]}
    #     #last_file=`echo "${files[@]:$((${#files[@]}-1))}"\
    #     #           | tr '/' ' ' | awk '{print $NF}'`
    # 	t=
    #     # if [ $compact_names == 1 ]
    #     # then
    #     #     t=`echo $last_file | cut -d '-' -f 2 | grep -o "[0-9]\{14\}"`
    #     # else
    #     #     t=`echo $last_file | cut -d '-' -f 2 | grep -o "[0-9]\{14,17\}"`000
    # 	#     t=${t:0:17}
    #     # fi
    #     last_file_date=$t
    #     # warc end date (from last file mtime). should (closely) 
    #     # correspond to time of last record in series
    #     # t=`stat --format=%x "${files[@]:$((${#files[@]}-1))}"\
    #     #   | cut -d '.' -f 1\
    #     #   | tr  -d '-'\
    #     #   | tr  -d ' '\
    #     #   | tr  -d ':'`
    # 	t=$(date +%Y%m%d%H%M%S -d @"$(stat -c %Y "$last_file")")
    # 	last_date=$t
    # 	end_date_ISO="$(set_tISO "$t")"
    # 	end_date_HR="$(set_tHR "$t")"
    # fi

    # if [ -z $end_date_ISO ]
    # then
    #     date_range=${start_date_ISO}
    # else
    #     date_range="${start_date_ISO} to ${end_date_ISO}"
    # fi
    date_range="${start_date_ISO} to ${end_date_ISO}"
}

function warc_software {
    cat=cat
    if [[ $1 =~ \.gz$ ]]; then
	cat=zcat
    fi
    $cat $1 2>/dev/null | awk '/^software:/{ print $2 } NR>1&&/^WARC[/]/{ exit }'
}
	
function echo_curl_output {
    echo "http://${s3}/${bucket}/${filename}" | tee -a $TASK
    echo "  response_code $1" | tee -a $TASK
    echo "  size_upload_bytes $2" | tee -a $TASK
    echo "  total_time_seconds $3" | tee -a $TASK
}

function write_success {
    SUCCESS_FLAG='true'
    if [ -e "$ERROR" ]
    then
        SUCCESS_FLAG='false'
        echo "ERROR file exists, could not write SUCCESS file." | tee -a $OPEN
    else
        for warc in `cat $MANIFEST | tr -s ' ' | cut -d ' ' -f 2`
        do
            if [ ! -f "${d}/${warc}.tombstone" ]
            then
                SUCCESS_FLAG='false'
                echo "ERROR: missing tombstone: ${d}/${warc}" | tee -a $OPEN
                cp $OPEN $ERROR
                exit 5
            fi
        done
        if [ $SUCCESS_FLAG == 'true' ]
        then
            echo "copying TASK file:" | tee -a $OPEN
            echo "    $TASK" | tee -a $OPEN
            echo "to SUCCESS file:" | tee -a $OPEN
            echo "    $SUCCESS" | tee -a $OPEN
            cp $TASK $SUCCESS

            echo "compiling TOMBSTONE:" | tee -a $OPEN
            echo "    $TOMBSTONE" | tee -a $OPEN
            cat ${d}/*.tombstone | tee -a $TOMBSTONE
        fi
    fi
}

function verify_etag {
    if [ -f $tmpfile ]
    then
        # this is a nice solution, thanks to Kenji
        # extract everything between quotes from Etag line, and 
        # immediately quit processing. it's far more efficient, 
        # and avoids the problem i had using grep + awk + tr 
        # which resulted in a ^M in the output, which failed 
        # the equality test below when it shouldn't have
        etag=`sed -ne '/ETag/{s/.*"\(.*\)".*/\1/p;q}' $tmpfile`
        if [ "$etag" != "$checksum" ]
        then
            # eventually want to retry, but let's see how often 
            # this happens
            echo "ERROR: bad ETag!"                   | tee -a $ERROR
            echo "  Content-MD5 request: '$checksum'" | tee -a $ERROR
            echo "  ETag response      : '$etag'"     | tee -a $ERROR
            #bad_etag=1
            #abort_series=1
            exit 1
	    return 1 # not reached
        else
            echo "ETag OK: $etag"
            echo "removing tmpfile: $tmpfile"
            rm $tmpfile
	    return 0
        fi
    else
        echo "Warning: tmpfile not found:" $tmpfile
	# assuming OK
	return 0
    fi
}

function write_tombstone {
    echo "writing download:" | tee -a $OPEN
    echo "  $download" | tee -a $OPEN
    echo "into tombstone:" | tee -a $OPEN
    echo "  $tombstone" | tee -a $OPEN
    echo $download > $tombstone
}

function schedule_retry {
    keep_trying='false'
    retry_epoch=$((`date +%s`+${retry_delay}))
    echo "RETRY: attempt (${retry_count}) scheduled"\
         "after ${retry_delay} seconds:"\
         `date +%Y-%m-%dT%T%Z -d @${retry_epoch}`\
         | tee -a $OPEN
    if [ ! -f $RETRY ]
    then
        echo $retry_epoch > $RETRY
    fi
    #abort_series=1
}

function curl_fake {
    printf '\n>>> THIS IS ONLY A TEST!<<<\n\n' >&2
    sleep 10
    # [response_code] [size_upload] [time_total]
    echo "200 000 000"
}

# 0 return code means curl succeeded
# 201 response_code means S3 succeeded
# output has the format:
#   [response_code] [size_upload] [time_total]
# /var/tmp/curl.out.$$ has the HTTP-header of the response

function check_curl_success {
    curl_status=$?
    if [ $curl_status == 0 ]
    then
        echo_curl_output $output
        echo "curl finished with status: $curl_status" | tee -a $OPEN
	response_code=${output/ */}
        if [ "$response_code" == "200" ] || [ "$response_code" == "201" ]
        then
    	    echo "SUCCESS: S3 PUT succeeded with response_code:"\
                 "$response_code" | tee -a $OPEN
	    case $upload_type in
	    auto-make-bucket)
		bucket_status=1;;
	    test-add-to-bucket)
		bucket_status=1
		echo "creating file: $BUCKET_OK"
	        touch $BUCKET_OK
		;;
	    *)
		if verify_etag; then
                    write_tombstone
		fi
	    esac
            keep_trying='false'
        else
            (( retry_count++ ))
    	    echo "ERROR: S3 PUT failed with response_code:"\
                 "$response_code at $(date +%Y-%m-%dT%T%Z)" \
                 | tee -a $OPEN
	    case $response_code in
	    000|4??|503)
		if [ $retry_count -gt $max_block_count ]
		then
		    echo "RETRY count ($retry_count) "\
			 "exceeds max_block_count: "\
			 $max_block_count\
			 | tee -a $OPEN
		    schedule_retry
		else
		    echo "BLOCK: sleep for ${block_delay} seconds..."\
			 | tee -a $OPEN
		    sleep ${block_delay}
		    echo "done sleeping at $(date +%Y-%m-%dT%T%Z)" \
			 | tee -a $OPEN
		fi
		;;
	    5??)
		schedule_retry;;
	    *)
		schedule_retry;;
	    esac
        fi
    else
        echo "ERROR: curl failed with status: $curl_status" | tee -a $OPEN
	echo_curl_output $output
        (( retry_count++ ))
	schedule_retry
    fi
}

function curl_s3 {
    if [ $retry_count -gt 0 ]
    then
        echo "RETRY attempt (${retry_count})" `date` | tee -a $OPEN
    fi
    curl_cmd=(
	$CURL -vv "${copts[@]}"
	http://${s3}/${bucket}/${filename}
	-o "$tmpfile"
    )
    echo "${curl_cmd[*]}" \
      | sed -e 's/--\(header\|write-out\|upload-file\)/\n  &/g'\
            -e 's/ http/\n  &/'\
            -e 's/w\?arc\(.gz\)\?,/&\n    /g'\
            -e 's/-o /\n  &/'\
      | tee -a $OPEN

    output=$("${curl_cmd[@]}")
    check_curl_success
}

################################################################

launch_count=0

CONFIG=$1
if [ -z "$CONFIG" ]; then
    echo Usage: $(basename $0) $usage
    exit 1
fi

force=$2
mode=$3

if [ -z $force ]; then force=0; fi
if [ -z $mode  ]; then mode=0;  fi

echo $(basename $0) $(date)

S3CFG=$HOME/.ias3cfg

if [ ! -f $CONFIG ]; then
    echo "ERROR: config not found: $CONFIG"
    exit 1
fi
if [ ! -f $S3CFG ]; then
    echo "ERROR: s3cfg not found: $S3CFG"
    exit 1
fi
# validate configuration
$BIN/config.py $CONFIG || {
    echo "ERROR: invalid config: $CONFIG"
    exit 1
}
xfer_job_dir=$($BIN/config.py $CONFIG xfer_dir)

crawljob=$($BIN/config.py $CONFIG crawljob)
compact_names=$($BIN/config.py $CONFIG compact_names)
warc_naming=$($BIN/config.py $CONFIG WARC_naming)
if ((compact_names)); then
    WARC_NAME_PATTERN='{prefix}-{timestamp}-{serial}'
else
    if ((warc_naming==1)); then
	WARC_NAME_PATTERN='{prefix}-{timestamp}-{serial}-{host}'
    else
	WARC_NAME_PATTERN='{prefix}-{timestamp}-{serial}-{pid}~{host}~{port}'
    fi
fi
for d in $(find $xfer_job_dir -mindepth 1 -maxdepth 1 -type d | sort)
do
  PACKED="$d/PACKED"
  MANIFEST="$d/MANIFEST"
  test -e "$MANIFEST" || continue

  OPEN="$d/LAUNCH.open"
  LAUNCH="$d/LAUNCH"
  RETRY="$d/RETRY"
  TASK="$d/TASK"
  ERROR="$d/ERROR"
  SUCCESS="$d/SUCCESS"
  TOMBSTONE="$d/TOMBSTONE"
  BUCKET_OK="$d/BUCKET_OK"
  tmpfile="/var/tmp/curl.out.$$"

  #warc_series=`echo $d | egrep -o '/([^/]*)$' | tr -d "/"`
  warc_series=$(basename $d)
  crawler=`echo $warc_series | awk -v FS=- '{print $NF}'`
  #crawler=`echo $warc_series | tr '-' ' ' | awk '{print $NF}'`
  crawldata="$d"

  # handle (5xx) RETRY file
  if [ -e $RETRY ]
  then
      retry_time=`cat $RETRY`
      echo "RETRY file exists: $RETRY [${retry_time}]" | tee -a $OPEN

      now=$(date +%s)
      if [ $now -lt $retry_time ]
      then
	  echo "  RETRY delay (now=${now} < retry_time=$retry_time)"\
	       | tee -a $OPEN
	  echo "    skipping series: $warc_series" | tee -a $OPEN
	  continue
      else
	  echo "  RETRY OK (now=$now > retry_time=$retry_time)"\
	       | tee -a $OPEN
	  echo "  moving aside RETRY file" | tee -a $OPEN
	  echorun mv $RETRY $RETRY.${retry_time} | tee -a $OPEN

	  echo "moving aside blocking files" | tee -a $OPEN
	  for blocker in $OPEN $ERROR $TASK
	  do
	      if [ -f $blocker ]
	      then
		  echorun mv $blocker "${blocker}.${retry_time}"
	      fi
	  done
      fi
  fi

  # check for files locking this series
  for lock_file in $OPEN $ERROR $LAUNCH $TASK; do
      if [ -e $lock_file ]; then
	  echo "$(basename $lock_file) file exists: $lock_file"
	  continue 2
      fi
  done

  echo "==== $warc_series ===="  | tee -a $OPEN
  echo "crawldata: $crawldata"   | tee -a $OPEN
  echo "mode: $mode"             | tee -a $OPEN
  echo "  CONFIG:    $CONFIG"    | tee -a $OPEN
  echo "  S3CFG:     $S3CFG"     | tee -a $OPEN
  echo "  MANIFEST:  $MANIFEST"  | tee -a $OPEN
  echo "  OPEN:      $OPEN"      | tee -a $OPEN
  echo "  TASK:      $TASK"      | tee -a $OPEN
  echo "  SUCCESS:   $SUCCESS"   | tee -a $OPEN
  echo "  TOMBSTONE: $TOMBSTONE" | tee -a $OPEN

  if [ $force == 0 ]; then query_user; fi

  # CURL S3 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  # parse MANIFEST for files to upload
  # --------------------------------------------------------------
  echo "parsing MANIFEST:" | tee -a $OPEN
  echo "  $MANIFEST" | tee -a $OPEN
  unset files
  for warc in $(awk '{print $2}' $MANIFEST)
  do
      file="${d}/${warc}"
      if [ -f "$file" ]
      then
	  files+=("$file")
      else
	  echo "ERROR: file not found: $file" | tee -a $OPEN
	  echo "Aborting!" | tee -a $OPEN
	  cp $OPEN $ERROR
	  exit 1
      fi
  done

  # check files found and build range string
  # --------------------------------------------------------------
  nfiles_manifest=$(grep -c '[^[:alnum:]]' $MANIFEST)
  nfiles_found=${#files[@]}

  echo "  nfiles_manifest = ${nfiles_manifest}" | tee -a $OPEN
  echo "  nfiles_found = ${nfiles_found}" | tee -a $OPEN

  if [ ${nfiles_manifest} -ne ${nfiles_found} ]; then
      echo "ERROR: count mis-match:"\
	   "nfiles_manifest=${nfiles_manifest} vs"\
	   "nfiles_found=${nfiles_found}"\
	   | tee -a $OPEN
      echo "Aborting!" | tee -a $OPEN
      cp $OPEN $ERROR
      exit 2
  fi

  first_file=${files[0]}
  last_file=${files[${#files[@]}-1]}

  # sets first_file_date, start_date_ISO, start_date_HR, scandate,
  # metadate, last_file_date, last_date, end_date_ISO, end_date_HR
  # and date_range
  set_date_range "$first_file" "$last_file"

  # get keys
  access_key=`grep access_key $S3CFG | awk '{print $3}'`
  secret_key=`grep secret_key $S3CFG | awk '{print $3}'`

  # parse config
  title_prefix=`$BIN/config.py $CONFIG title_prefix`
  # test_suffix=`$BIN/config.py $CONFIG test_suffix`
  block_delay=`$BIN/config.py $CONFIG block_delay`
  max_block_count=`$BIN/config.py $CONFIG max_block_count`
  retry_delay=`$BIN/config.py $CONFIG retry_delay`

  # parse series
  #ws_date=`echo $warc_series | cut -d '-' -f 2`
  #cdate=`echo ${ws_date:0:6}`

  # bucket metadata
  bucket="${warc_series}${test_suffix}"
  mediatype='web'
  title="${title_prefix} ${date_range}"
  subject='crawldata'
  derive=`$BIN/config.py $CONFIG derive`

  num_warcs=${nfiles_manifest}
  size_hint=`cat $PACKED | awk '{print $NF}'`
  first_serial=$(parse_warc_name "$(basename "$first_file")"; echo $serial)
  last_serial=$(parse_warc_name "$(basename "$last_file")"; echo $serial)
  # if [ $compact_names == 1 ]
  # then
  #     first_serial=`echo $first_file\
  # 	| cut -d '-' -f 3 | sed -e 's/.warc.gz//'`
  #     last_serial=` echo $last_file\
  # 	| cut -d '-' -f 3 | sed -e 's/.warc.gz//'`
  # else
  #     first_serial=`echo $warc_series | cut -d '-' -f 3`
  #     last_serial=` echo $warc_series | cut -d '-' -f 4`
  # fi

  # metadata per BK, intended to be like books
  #   Subject: metadata for the web stuff going into the paired archive
  #   Date:  2010-09-18T12:16:00PDT
  scanner=`hostname -f`
  creator=`$BIN/config.py $CONFIG creator`
  sponsor=`$BIN/config.py $CONFIG sponsor`
  contributor=`$BIN/config.py $CONFIG contributor`
  # scandate (using 14-digits of timestamp of first warc in series)
  # metadate (like books, the year)
  operator=`$BIN/config.py $CONFIG operator`
  scancenter=`$BIN/config.py $CONFIG scanningcenter`
  access="http://www.archive.org/details/${bucket}"
  crawler_version=$(warc_software ${files[0]})
  description=`$BIN/config.py $CONFIG description\
    | sed -e s/CRAWLHOST/$scanner/\
      -e s/CRAWLJOB/$crawljob/\
      -e s/START_DATE/"$start_date_HR"/\
      -e s/END_DATE/"$end_date_HR"/\
    | tr -s ' '`

  # support multiple arbitrary collections
  # webwidecrawl/collection/serial
  #   => collection3 = webwidecrawl
  #   => collection2 = collection
  #   => collection1 = serial
  COLLECTIONS=()
  colls=($($BIN/config.py $CONFIG collections | tr '/' ' '))
  coll_count=${#colls[@]}
  for c in ${colls[@]}
  do
      # --header 'x-archive-meta03-collection:${collection1}'\
      # --header 'x-archive-meta02-collection:${collection2}'\
      # --header 'x-archive-meta01-collection:${collection3}'\
      collection[$coll_count]=$c
      COLLECTIONS+=(
	  --header
	  "$(printf 'x-archive-meta%02d-collection' $coll_count):$c"
      )
      ((coll_count--))
  done

  if [ -z "$creator" -o -z "$sponsor" -o -z "$contributor" -o \
       -z "$description" -o -z "$scancenter" -o -z "$operator" ]; then
      echo "ERROR some null metadata." | tee -a $OPEN
      echo "Aborting." | tee -a $OPEN
      exit 1          
  fi

  echo "[item metadata]"
  echo "  mediatype     = $mediatype"
  echo "  title         = $title"
  echo "  description   = $description"
  echo "  collection(s) = ${collection[@]}"
  echo "  subject       = $subject"
  echo "[books metadata]"
  echo "  scanningcenter = '${scancenter}'"
  echo "  scandate       = '${scandate}'"
  echo "  scanner        = '${scanner}'"
  echo "  date           = '${metadate}'"
  echo "  creator        = '${creator}'"
  echo "  sponsor        = '${sponsor}'"
  echo "  contributor    = '${contributor}'"
  echo "  operator       = '${operator}'"
  echo "  id...-access   = '${access}'"
  echo "[crawl metadata]"
  echo "  crawler         = ${crawler_version}"
  echo "  crawljob        = $crawljob"
  echo "  numwarcs        = $num_warcs"
  echo "  sizehint        = $size_hint"
  echo "  firstfileserial = $first_serial"
  echo "  firstfiledate   = $first_file_date"
  echo "  lastfileserial  = $last_serial"
  echo "  lastfiledate    = $last_file_date"
  echo "  lastdate        = $last_date"

  if [ $force == 0 ]; then query_user; fi

  # 1) create new item with MANIFEST, metadata (auto-make-bucket)
  # --------------------------------------------------------------
  common_opts=(
      --include --location
      --header "authorization: LOW ${access_key}:${secret_key}"
      --write-out '%{http_code} %{size_upload} %{time_total}'
  )
  derive_opts=(--header 'x-archive-queue-derive:1')
  noderive_opts=(--header 'x-archive-queue-derive:0')
  automakebucket_opts=(--header 'x-amz-auto-make-bucket:1')
  sizehint_opts=(--header "x-archive-size-hint:${size_hint}")
  itemmeta_opts=(
      --header "x-archive-meta-mediatype:${mediatype}"
      --header "x-archive-meta-title:${title}"
      --header "x-archive-meta-description:${description}"
      --header "x-archive-meta-subject:${subject}"
      --header "x-archive-meta-scanner:${scanner}"
      --header "x-archive-meta-creator:${creator}"
      --header "x-archive-meta-scandate:${scandate}"
      --header "x-archive-meta-date:${metadate}"
      --header "x-archive-meta-sponsor:${sponsor}"
      --header "x-archive-meta-contributor:${contributor}"
      --header "x-archive-meta-scanningcenter:${scancenter}"
      --header "x-archive-meta-operator:${operator}"
      --header "x-archive-meta-identifier-access:${access}"
      --header "x-archive-meta-crawler:${crawler_version}"
      --header "x-archive-meta-crawljob:${crawljob}"
      --header "x-archive-meta-numwarcs:${num_warcs}"
      --header "x-archive-meta-sizehint:${size_hint}"
      --header "x-archive-meta-firstfileserial:${first_serial}"
      --header "x-archive-meta-firstfiledate:${first_file_date}"
      --header "x-archive-meta-lastfileserial:${last_serial}"
      --header "x-archive-meta-lastfiledate:${last_file_date}"
      --header "x-archive-meta-lastdate:${last_date}"
  )

  filepath=$MANIFEST
  filename="$(basename $filepath).txt"
  copts=(
      "${common_opts[@]}"
      "${itemmeta_opts[@]}"
      "${COLLECTIONS[@]}"
      "${noderive_opts[@]}"
      "${automakebucket_opts[@]}"
      "${sizehint_opts[@]}"
      --upload-file "$filepath"
  )
  retry_count=0
  keep_trying=true
  upload_type="auto-make-bucket"
  bucket_status=0
  if [ -f $BUCKET_OK ]; then
      echo "BUCKET_OK exists, skipping $upload_type"
  else
      echo "Creating item: http://archive.org/details/${bucket}" | tee -a $OPEN
      while $keep_trying; do
	  echo "-----" | tee -a $OPEN
	  curl_s3
	  # curl_s3 sets keep_trying to false for success
      done
      # if auto-make-bucket has failed, do not try to upload warcs
      if [ $bucket_status -eq 0 ]; then
	  echo "Create (auto-make-bucket) failed: $bucket" | tee -a $OPEN
	  echo "aborting series: $warc_series" | tee -a $OPEN
	  continue
      fi
      echo "item/bucket created successfully: $bucket" | tee -a $OPEN
  fi
  #abort_series=0
  #bad_etag=0

  # 2) run HEAD on item URL to make sure item is ready
  # item creation request above may be sitting in the queue for a while.
  # ----------------------------------------------------------------
  if [ -f $BUCKET_OK ]; then
      echo "BUCKET_OK exists, skipping bucket check"
  else
      filename='' # makes request URL bucket name + '/'
      retry_count=0
      keep_trying=true
      upload_type="test-add-to-bucket"
      copts=(
	  "${common_opts[@]}"
	  --head
      )
      while $keep_trying; do
	  echo "Checking if bucket ${bucket} exists" | tee -a $OPEN
	  curl_s3
      done
      if [ -f $RETRY ]; then
	  echo "Aborting warc_series: $warc_series" | tee -a $OPEN
	  continue
      fi
  fi

  # 3) add WARCs to newly created item (upload-file)
  # ----------------------------------------------------------------
  echo "----" | tee -a $OPEN
  echo "Uploading (${num_warcs}) warcs with size hint: $size_hint bytes"\
    | tee -a $OPEN
  for (( i = 0 ; i < ${#files[@]} ; i++ )) # ADD-TO-ITEM loop
  do
      filepath=${files[$i]}
      filename=$(basename "$filepath")
      checksum=`grep $filename $MANIFEST | awk '{print $1}'`
      download="http://${dl}/${bucket}/${filename}"
      tombstone="${filepath}.tombstone"
      retry_count=0
      keep_trying=true
      upload_type="add-to-bucket"
      derive_header=(--header 'x-archive-queue-derive:0')
      while $keep_trying # RETRY loop
      do
	  printf -- '----\n[%d/%d]: %s\n' $((i+1)) ${#files[@]} "$filename" \
	      | tee -a $OPEN

	  if [ -f $tombstone ]
	  then
	      echo "tombstone exists, skipping upload: $tombstone"
	      keep_trying=false
	  else
	      # turn on derive on the last file UNLESS drive is disabled
	      if [ $((i+1)) -eq ${#files[@]} -a $derive -eq 1 ]
	      then
		  derive_header=()
	      fi
	      copts=(
		  "${common_opts[@]}"
		  --header "Content-MD5:${checksum}"
		  "${derive_header[@]}"
		  --upload-file "${filepath}"
	      )
	      curl_s3
	  fi

	  #if [ $abort_series -eq 1 ]
	  #then
	  if [ -f $RETRY ]; then
	       echo "Aborting warc_series: $warc_series" | tee -a $OPEN
	       continue 3
	  fi

      done
  done


  # /CURL S3 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

  # write success, tombstone files
  if [ $TEST == "false" ]
  then
      write_success
  fi

  # unlock process
  echo "mv open file to LAUNCH: $LAUNCH"
  mv $OPEN $LAUNCH || {
    error_msg="ERROR: failed to mv $OPEN to $LAUNCH"
    echo $error_msg | tee -a $ERROR
    exit 4
  }

  (( launch_count++ ))

  # check mode
  if [ $mode == 'single' ]
  then
    echo "$launch_count buckets filled"
    echo "mode = $mode, exiting normally."
    echo `basename $0` "done." `date`
    exit 0
  fi

  unset OPEN
  unset LAUNCH
  unset RETRY
  unset TASK
  unset ERROR
  unset SUCCESS
  unset COLLECTIONS

done

echo "$launch_count buckets filled"
echo `basename $0` "done." `date`

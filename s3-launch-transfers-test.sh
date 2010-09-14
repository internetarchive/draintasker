#!/bin/bash
# s3-launch-transfers-test.sh xfer_job_dir [force] [mode]
#
# foreach warc_series in xfer_job_dir, submit an S3 HTTP request
# to upload files to remote storage - unless one of the 
# following files exists: LAUNCH, LAUNCH.open, ERROR, TASK
#
#  xfer_job_dir   /{rsync_path}/{JOB}
#  [force]        optionally skip interactive continue
#  [mode]         single = submit 1 task, then exit
#
# DEPENDENCIES
#
#   CONFIG  ./dtmon.cfg
#           access_key
#           secret_key
#           title_prefix
#           collection_prefix
#           test_suffix
#           retry_delay_4xx
#           retry_delay_5xx
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

usage="$0 xfer_job_dir [force] [mode=single]"

TEST='true'
s3='s3.us.archive.org'
dl='www.archive.org/download'
std_warc_size=$(( 1024*1024*1024 )) # 1 gibibyte

################################################################

function query_user {
  echo "Continue [Y/n]> "
  read text
  if [ "$text" != "Y" ]
  then
    echo "Aborting."
    exit 1
  fi
}

function set_t14 { # 14-digit timestamp
    t14=`echo ${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}`
}

function set_tHR { # human-readable date format
    format_date='date "+%T %b%e %G" -d "`echo ${t14}`"'
    # eg '20:13:02 Sep 1 2010'
    tHR=`eval $format_date`
}

# ISO 8601 date format
function set_tISO {
    # sort of hard-coded, could use date +%Y-%m-%dT%H:%M:%S%Z -d blah
    tISO=`echo ${t:0:4}-${t:4:2}-${t:6:2}T${t:8:2}:${t:10:2}:${t:12:2}Z`
}

# we could do something more sophisticated here for more
# human readable date ranges
function set_date_range {

    # get warc start date
    t=`echo $files[0]\
      | cut -d '-' -f 2\
      | grep -o "[0-9]\{17\}"`
    set_tISO
    start=${tISO}

    # get warc end date
    if [ ${#files[@]} -gt 1 ]
    then
        t=`echo "${files[@]:$((${#files[@]}-1))}"\
            | tr '/' ' '\
            | awk '{print $NF}'\
            | cut -d '-' -f 2\
            | grep -o "[0-9]\{17\}"`
        set_tISO
        end=${tISO}
    fi

    if [ -z $end ]
    then
        date_range=${start}
    else
        date_range="${start} to ${end}"
    fi
}

function echo_curl_output {
    response_code=`echo $output | awk '{print $((NF-2))}'`
    size_upload_bytes=`echo $output | awk '{print $((NF-1))}'`
    total_time_seconds=`echo $output | awk '{print $NF}'`
    echo "http://${s3}/${bucket}/${filename}" | tee -a $TASK
    echo "  response_code $response_code" | tee -a $TASK
    echo "  size_upload_bytes $size_upload_bytes" | tee -a $TASK
    echo "  total_time_seconds $total_time_seconds" | tee -a $TASK
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

function write_tombstone {
    echo "writing download:" | tee -a $OPEN
    echo "  $download" | tee -a $OPEN
    echo "into tombstone:" | tee -a $OPEN
    echo "  $tombstone" | tee -a $OPEN
    echo $download > $tombstone
}

# 0 return code means curl succeeded
# 201 response_code means S3 succeeded
# output has the format:
#   [response_code] [size_upload] [time_total]
# /var/tmp/curl.out.$$ has the HTTP-header of the response

function check_curl_success {
    if [ $? == 0 ]
    then
        echo_curl_output
        echo "curl finished with status: $?" | tee -a $OPEN
        response_code=`echo $output | cut -d ' ' -f 1`
        if [ "$response_code" == "201" ] # SUCCESS
        then
    	    echo "SUCCESS: S3 PUT succeeded with response_code:"\
                 "$response_code" | tee -a $OPEN
	    if [ $upload_type == "auto-make-bucket" ]
            then 
                bucket_status=1
            else
                write_tombstone
	    fi
            keep_trying='false'
        else
    	    echo "ERROR: S3 PUT failed with response_code:"\
                 "$response_code at "`date +%Y-%m-%dT%T%Z`\
                 | tee -a $OPEN
	    if [ ${response_code} == "000" ] # curl failed with status 0!
            then
                (( retry_count++ ))
                echo "RETRY: sleep for ${retry_delay_4xx} seconds..."\
                     | tee -a $OPEN
		sleep ${retry_delay_4xx}
		echo "done sleeping at "`date +%Y-%m-%dT%T%Z`\
                     | tee -a $OPEN
	    elif [ ${response_code:0:1} == "4" ] # 4xx
            then
                (( retry_count++ ))
                echo "RETRY: sleep for ${retry_delay_4xx} seconds..."\
                     | tee -a $OPEN
		sleep ${retry_delay_4xx}
		echo "done sleeping at "`date +%Y-%m-%dT%T%Z`\
                     | tee -a $OPEN
            elif [ ${response_code:0:1} == "5" ] # 5xx
            then
		keep_trying='false'
                (( retry_count++ ))
		retry_epoch=$((`date +%s`+${retry_delay_5xx}))
		echo "RETRY: attempt (${retry_count}) scheduled"\
                     "after ${retry_delay_5xx} seconds:"\
                     `date +%Y-%m-%dT%T%Z -d @${retry_epoch}`\
                     | tee -a $OPEN
		if [ ! -f $RETRY ]
                then
		    echo $retry_epoch > $RETRY
                fi
            else
                echo "Aborting!" | tee -a $OPEN
                mv $TASK $ERROR
                exit 3
            fi
        fi
    else
	echo_curl_output

        echo "ERROR: curl failed with status: $?" | tee -a $OPEN
        # echo "Aborting!" | tee -a $OPEN
        # cp $OPEN $ERROR
        # exit 4

	# retry on curl failure
        keep_trying='false'
        (( retry_count++ ))
        retry_epoch=$((`date +%s`+${retry_delay_5xx}))
        echo "RETRY: attempt (${retry_count}) scheduled"\
             "after ${retry_delay_5xx} seconds, at"\
             `date +%Y-%m-%dT%T%Z -d @${retry_epoch}`\
             | tee -a $OPEN
        echo $retry_epoch > $RETRY

    fi
}

function curl_s3 {
    if [ $retry_count -gt 0 ]
    then
        echo "RETRY attempt (${retry_count})" `date` | tee -a $OPEN
    fi
    echo "curl ${copts} http://${s3}/${bucket}/${filename} -o $tmpfile"\
      | sed -e 's/--/\n  --/g'\
      | sed -e 's/ http/\n  http/'\
      | sed -e 's/warc.gz,/warc.gz,\n    /g'\
      | sed -e 's/-o /\n  -o /'\
      | tee -a $OPEN
    curl_cmd="curl -vv ${copts} http://${s3}/${bucket}/${filename} -o $tmpfile"
    if [ $TEST == "false" ]
    then
        output=`eval ${curl_cmd}`
        check_curl_success
    else
        echo | tee -a $OPEN
        echo ">>> THIS IS ONLY A TEST! <<<" | tee -a $OPEN
        echo | tee -a $OPEN
	keep_trying='false'
	bucket_status=1
    fi
}

################################################################

launch_count=0

if [ -n "$1" ]
then

  if [ "$2" == "1" ]; then force=1; else force=0; fi
  if [ -n "$3" ]; then mode=$3; else mode='kickass'; fi

  echo $0 `date`

  xfer_job_dir="$1"

  back=`pwd`
  cd $1

  for d in `find $xfer_job_dir -type d | sort`
  do

    MANIFEST="$d/MANIFEST"
    if [ -e "$MANIFEST" ]
    then

      CONFIG="${back}/dtmon.cfg"
      OPEN="$d/LAUNCH-test.open"
      LAUNCH="$d/LAUNCH-test"
      RETRY="$d/RETRY-test"
      TASK="$d/TASK-test"
      ERROR="$d/ERROR-test"
      SUCCESS="$d/SUCCESS-test"
      TOMBSTONE="$d/TOMBSTONE-test"
      tmpfile="/var/tmp/curl.out.$$"

      warc_series=`echo $d | egrep -o '/([^/]*)$' | tr -d "/"`
      crawler=`echo $warc_series | tr '-' ' ' | awk '{print $NF}'`
      crawljob=`echo $d | grep -o incoming/[^/]* | cut -d '/' -f 2`
      crawldata="$d"

      # handle (5xx) RETRY file
      if [ -e $RETRY ]
      then
	  echo "RETRY file exists with epoch: "`cat $RETRY`

          now=`date +%s`
          retry_time=`cat $RETRY`

          if [ $now -lt $retry_time ]
          then
	      echo "RETRY delay (now=${now} < retry_time=$retry_time)"\
                   | tee -a $OPEN
	      echo "exiting normally." | tee -a $OPEN
	      exit 0
          else
              echo "RETRY OK (now=$now > retry_time=$retry_time)" | tee -a $OPEN

              echo "moving aside RETRY file" | tee -a $OPEN
              echo "mv $RETRY" | tee -a $OPEN
              echo "   $RETRY.${retry_time}" | tee -a $OPEN
	      mv $RETRY $RETRY.${retry_time}

	      echo "moving aside blocking files" | tee -a $OPEN
	      for blocker in $OPEN $ERROR $TASK
              do
	          aside="${blocker}.${retry_time}"
		  if [ -f $blocker ]
                  then
	              echo "mv $blocker"
	              echo "   $aside"
		      mv $blocker $aside
                  fi
              done
          fi
      fi

      # check for files locking this series
      locking_files=($OPEN $ERROR $LAUNCH $TASK)
      locking_keys=(LAUNCH.open ERROR LAUNCH TASK)
      n_lock_files=${#locking_files[@]}
      for (( l=0; l < n_lock_files ; l++ ))
      do
          lock=${locking_keys[$l]}
          lock_file=${locking_files[$l]}
          non_test_lock_file=$d/${locking_keys[$l]}
          if [ -e $lock_file ]
          then
              echo "  $lock file exists: $lock_file"
              continue 2
          elif [ -e $non_test_lock_file ]
          then
              echo "  $lock file exists: $non_test_lock_file"
              continue 2
          fi
      done

      echo "==== $warc_series ===="  | tee -a $OPEN
      echo "crawldata: $crawldata"   | tee -a $OPEN
      echo "mode: $mode"             | tee -a $OPEN
      echo "  CONFIG:    $CONFIG"    | tee -a $OPEN
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
      num_warcs=0
      for warc in `cat $MANIFEST | tr -s ' ' | cut -d ' ' -f 2`
      do
          file="${d}/${warc}"
          if [ -f "$file" ]
          then
              files[$num_warcs]=$file
              (( num_warcs++ ))
          else
      	      echo "ERROR: file not found: $file" | tee -a $OPEN
      	      echo "Aborting!" | tee -a $OPEN
      	      cp $OPEN $ERROR
      	      exit 1
          fi
      done

      # check files found and build range string
      # --------------------------------------------------------------
      nfiles_manifest=`grep [^[:alnum:]] $MANIFEST | wc -l`
      nfiles_found=${#files[@]}

      echo "  nfiles_manifest = ${nfiles_manifest}" | tee -a $OPEN
      echo "  nfiles_found = ${nfiles_found}" | tee -a $OPEN

      if [ ${nfiles_manifest} -ne ${nfiles_found} ]
      then
          echo "ERROR: count mis-match:"\
               " nfiles_manifest=${nfiles_manifest} vs "\
               " nfiles_found=${nfiles_found}"\
               | tee -a $OPEN
          echo "Aborting!" | tee -a $OPEN
          cp $OPEN $ERROR
          exit 2
      else
	  set_date_range
      fi

      # parse config
      access_key=`grep ^s3_access_key $CONFIG  | tr -s ' ' | cut -d ' ' -f 2`
      secret_key=`grep ^s3_secret_key $CONFIG  | tr -s ' ' | cut -d ' ' -f 2`
      title_prefix=`grep ^title_prefix $CONFIG | tr -s ' ' | cut -d '"' -f 2`
      collection_prefix=`grep ^collection_prefix $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
      test_suffix=`grep ^test_suffix $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
      retry_delay_4xx=`grep ^retry_delay_4xx $CONFIG | tr -s ' ' | cut -d ' ' -f 2`
      retry_delay_5xx=`grep ^retry_delay_5xx $CONFIG | tr -s ' ' | cut -d ' ' -f 2`

      # parse series
      ws_date=`echo $warc_series | cut -d '-' -f 2`
      cdate=`echo ${ws_date:0:6}`

      # get reports
      reports="No reports found for crawljob: $crawljob"

      # bucket metadata
      bucket="${warc_series}${test_suffix}"
      mediatype='web'
      title="${title_prefix} ${date_range}"
      description=${reports}
      collection1="${collection_prefix}_${cdate}"
      collection2='webwidecrawl'
      collection3='test_collection'
      subject='crawldata'
      derive=0
      num_warcs=${nfiles_manifest}
      size_hint=$(( $std_warc_size * $num_warcs )) # in gibibytes
      start_serial=`echo $warc_series | cut -d '-' -f 3`
      start_date=${start}
      end_serial=`echo $warc_series | cut -d '-' -f 4`
      end_date=${end}

      echo "metadata                      "
      echo "  bucket       = $bucket      "
      echo "  mediatype    = $mediatype   "
      echo "  title        = $title       "
      echo "  description  = $description "
      echo "  collection1  = $collection1 "
      echo "  collection2  = $collection2 "
      echo "  collection3  = $collection3 "
      echo "  subject      = $subject     "
      echo "  derive       = $derive      "
      echo "  num_warcs    = $num_warcs   "
      echo "  size_hint    = $size_hint   "
      echo "  start_serial = $start_serial"
      echo "  start_date   = $start_date  "
      echo "  end_serial   = $end_serial  "
      echo "  end_date     = $end_date    "

      echo "Creating item: http://archive.org/details/${bucket}" | tee -a $OPEN

      if [ $force == 0 ]; then query_user; fi

      # 1) create new item with MANIFEST, metadata (auto-make-bucket)
      # --------------------------------------------------------------
      filepath=$MANIFEST
      filename=`echo $filepath | grep -o "[^/]*$"`".txt"
      echo "----" | tee -a $OPEN
      copts="--include --location\
       --header 'x-amz-auto-make-bucket:1'\
       --header 'x-archive-meta-mediatype:${mediatype}'\
       --header 'x-archive-meta01-collection:${collection1}'\
       --header 'x-archive-meta02-collection:${collection2}'\
       --header 'x-archive-meta03-collection:${collection3}'\
       --header \"x-archive-meta-title:${title}\"\
       --header \"x-archive-meta-description:${description}\"\
       --header 'x-archive-meta-subject:${subject}'\
       --header 'x-archive-meta-crawler:${crawler}'\
       --header 'x-archive-meta-num_warcs:${num_warcs}'\
       --header 'x-archive-meta-size_hint:${size_hint}'\
       --header 'x-archive-meta-start_serial:${start_serial}'\
       --header 'x-archive-meta-start_date:${start_date}'\
       --header 'x-archive-meta-end_serial:${end_serial}'\
       --header 'x-archive-meta-end_date:${end_date}'\
       --header 'x-archive-queue-derive:${derive}'\
       --header 'x-archive-size-hint:${size_hint}'\
       --header \"authorization: LOW ${access_key}:${secret_key}\"\
       --write-out '%{http_code} %{size_upload} %{time_total}'\
       --upload-file ${filepath}"
      retry_count=0
      keep_trying='true'
      upload_type="auto-make-bucket"
      bucket_status=0
      until [ $keep_trying == 'false' ] # RETRY loop
      do
          curl_s3
      done

      # if auto-make-bucket has failed, do not try to upload warcs
      if [ $bucket_status -eq 0 ]
      then
	  echo "Create (auto-make-bucket) failed: $bucket" | tee -a $OPEN
	  echo "aborting series: $warc_series" | tee -a $OPEN
	  continue
      else
          echo "item/bucket created successfully: $bucket" | tee -a $OPEN
      fi

      # 2) add WARCs to newly created item (upload-file)
      # ----------------------------------------------------------------
      echo "----" | tee -a $OPEN
      echo "Uploading (${num_warcs}) warcs with size hint: $size_hint bytes"\
        | tee -a $OPEN
      for (( i = 0 ; i < ${#files[@]} ; i++ )) # ADD-TO-ITEM loop
      do
          filepath=${files[$i]}
          filename=`echo $filepath | grep -o "[^/]*$"`
          download="http://${dl}/${bucket}/${filename}"
          tombstone="${filepath}.tombstone"
          retry_count=0
          keep_trying='true'
          upload_type="add-file-to-bucket"
          until [ $keep_trying == 'false' ] # RETRY loop
          do
              echo "----" | tee -a $OPEN
              echo "["$((${i}+1))"/${#files[@]}]: ${filename}" | tee -a $OPEN
              copts="--include --location\
                     --header \"authorization: LOW ${access_key}:${secret_key}\"\
                     --header 'x-archive-queue-derive:${derive}'\
                     --write-out '%{http_code} %{size_upload} %{time_total}'\
                     --upload-file ${filepath}"
              curl_s3
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
      mv $OPEN $LAUNCH
      if [ $? != 0 ]
      then
        error_msg="ERROR: failed to mv $OPEN to $LAUNCH"
	echo $error_msg | tee -a $ERROR
        exit 4
      fi

      (( launch_count++ ))

      # check mode
      if [ $mode == 'single' ]
      then
        echo "$launch_count buckets filled"
        echo "mode = $mode, exiting normally."
        exit 0
      fi

    fi # /if [ -e MANIFEST ]

    unset files
    unset OPEN
    unset LAUNCH
    unset RETRY
    unset TASK
    unset ERROR
    unset SUCCESS

  done

  echo "$launch_count buckets filled"
  echo $0 "Done." `date`

  cd $back

else
  echo $usage
  exit 1
fi



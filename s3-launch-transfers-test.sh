#!/bin/bash
#
# s3-launch-transfers-test.sh config [force] [mode]
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

usage="config [force] [mode=single]"

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

function set_tHR { # human-readable date format
    tHR=`date -d "${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}"`
}

# ISO 8601 date format
function set_tISO {
    # tISO=`echo ${t:0:4}-${t:4:2}-${t:6:2}T${t:8:2}:${t:10:2}:${t:12:2}Z`
    tISO=`date +%Y-%m-%dT%T%Z -d "${t:0:4}-${t:4:2}-${t:6:2} ${t:8:2}:${t:10:2}:${t:12:2}"`
}

# we could do something more sophisticated here
# for more human readable date ranges
function set_date_range {

    # get warc start date
    t=`echo $files[0]\
      | cut -d '-' -f 2\
      | grep -o "[0-9]\{17\}"`
    first_file_date=$t
    set_tISO
    start_date_ISO=${tISO}
    set_tHR
    start_date_HR=${tHR}

    scandate=${t:0:14}
    metadate=${t:0:4}
    
    # get dates from last file in series
    if [ ${#files[@]} -gt 1 ]
    then
        last_file="${files[@]:$((${#files[@]}-1))}"
        t=`echo "${files[@]:$((${#files[@]}-1))}"\
            | tr '/' ' '\
            | awk '{print $NF}'\
            | cut -d '-' -f 2\
            | grep -o "[0-9]\{17\}"`
        last_file_date=$t
        # warc end date (from last file mtime). should (closely) 
        # correspond to time of last record in series
        t=`stat --format=%y "$last_file"\
          | cut -d '.' -f 1\
          | tr  -d '-'\
          | tr  -d ' '\
          | tr  -d ':'`
        last_date=${t:0:14}
        set_tISO
        end_date_ISO=${tISO}
        set_tHR
        end_date_HR=${tHR}
    fi

    if [ -z $end_date_ISO ]
    then
        date_range=${start_date_ISO}
    else
        date_range="${start_date_ISO} to ${end_date_ISO}"
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
        if [ $etag != $checksum ]
        then
            # eventually want to retry, but let's see how often 
            # this happens
            echo "ERROR: bad ETag!"                   | tee -a $ERROR
            echo "  Content-MD5 request: '$checksum'" | tee -a $ERROR
            echo "  ETag response      : '$etag'"     | tee -a $ERROR
            bad_etag=1
            abort_series=1
            exit 1
        else
            echo "ETag OK: $etag"
            echo "removing tmpfile: $tmpfile"
            rm $tmpfile
        fi
    else
        echo "Warning: tmpfile not found:" $tmpfile
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
    abort_series=1
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
		verify_etag
		if [ $bad_etag -eq 0 ]
                then
                    write_tombstone
		fi
	    fi
            keep_trying='false'
        else
            (( retry_count++ ))
    	    echo "ERROR: S3 PUT failed with response_code:"\
                 "$response_code at "`date +%Y-%m-%dT%T%Z`\
                 | tee -a $OPEN
            if [ ${response_code} == "000" ] || [ ${response_code:0:1} == "4" ]
            then
		# curl failed with status 0! OR 4xx HTTP response
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
		    echo "done sleeping at "`date +%Y-%m-%dT%T%Z`\
                         | tee -a $OPEN
                fi
            elif [ ${response_code:0:1} == "5" ]
            then
                # 5xx HTTP response
		schedule_retry
            else
                # un-handled HTTP response
		schedule_retry
            fi
        fi
    else
        echo "ERROR: curl failed with status: $?" | tee -a $OPEN
	echo_curl_output
        (( retry_count++ ))
	schedule_retry
    fi
}

function curl_s3 {
    if [ $retry_count -gt 0 ]
    then
        echo "RETRY attempt (${retry_count})" `date` | tee -a $OPEN
    fi
    echo "curl ${copts} http://${s3}/${bucket}/${filename} -o $tmpfile"\
      | sed -e 's/--header/\n  --header/g'\
      | sed -e 's/--write-out/\n  --write-out/g'\
      | sed -e 's/--upload-file/\n  --upload-file/g'\
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

	if [ $upload_type == "add-to-bucket" ]
        then
            # sleep 5
            # curl blah
	    # output="504 000 000"
            output="201 000 000"
            check_curl_success
        else
            # sleep 3
	    output="201 000 000"
	    # curl blah
            check_curl_success
        fi
    fi
}

################################################################

launch_count=0

if [ -n "$1" ]
then

  force=$2
  mode=$3

  if [ -z $force ]; then force=0; fi
  if [ -z $mode  ]; then mode=0;  fi

  echo `basename $0` `date`

  CONFIG=$1
  S3CFG=$HOME/.ias3cfg

  if [ ! -f $CONFIG ] && [ ! -f $S3CFG ]
  then
      echo "ERROR: config or s3cfg not found"
      exit 1
  elif [ ${CONFIG:0:1} != '/' ]
  then
      echo "ERROR: must give fullpath for config: $CONFIG"
      exit 1
  else
      # validate configuration
      config.py $CONFIG
      if [ $? != 0 ]
      then
          echo "ERROR: invalid config: $CONFIG"
          exit 1
      fi
      xfer_job_dir=`config.py $CONFIG xfer_dir`
  fi

  for d in `find $xfer_job_dir -type d | sort`
  do

    PACKED="$d/PACKED"
    MANIFEST="$d/MANIFEST"
    if [ -e "$MANIFEST" ]
    then

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
      crawljob=`config.py $CONFIG crawljob`
      crawldata="$d"

      # handle (5xx) RETRY file
      if [ -e $RETRY ]
      then
	  echo "  RETRY file exists: $RETRY ["`cat $RETRY`"]"\
               | tee -a $OPEN

          now=`date +%s`
          retry_time=`cat $RETRY`

          if [ $now -lt $retry_time ]
          then
	      echo "    RETRY delay (now=${now} < retry_time=$retry_time)"\
                   | tee -a $OPEN
	      echo "    skipping series: $warc_series" | tee -a $OPEN
	      continue
          else
              echo "RETRY OK (now=$now > retry_time=$retry_time)"\
                   | tee -a $OPEN
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
              echo "$lock file exists: $lock_file"
              continue 2
          elif [ -e $non_test_lock_file ]
          then
              echo "$lock file exists: $non_test_lock_file"
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
      num_warcs=0
      unset files
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
               "nfiles_manifest=${nfiles_manifest} vs"\
               "nfiles_found=${nfiles_found}"\
               | tee -a $OPEN
          echo "Aborting!" | tee -a $OPEN
          cp $OPEN $ERROR
          exit 2
      else
	  set_date_range
      fi

      # get keys
      access_key=`grep access_key $S3CFG | awk '{print $3}'`
      secret_key=`grep secret_key $S3CFG | awk '{print $3}'`

      # parse config
      title_prefix=`   config.py $CONFIG title_prefix`
      # test_suffix=`    config.py $CONFIG test_suffix`
      block_delay=`    config.py $CONFIG block_delay`
      max_block_count=`config.py $CONFIG max_block_count`
      retry_delay=`    config.py $CONFIG retry_delay`

      # parse series
      ws_date=`echo $warc_series | cut -d '-' -f 2`
      cdate=`echo ${ws_date:0:6}`

      # bucket metadata
      bucket="${warc_series}${test_suffix}"
      mediatype='web'
      title="${title_prefix} ${date_range}"
      subject='crawldata'
      derive=0

      num_warcs=${nfiles_manifest}
      size_hint=`cat $PACKED | awk '{print $NF}'`
      first_serial=`echo $warc_series | cut -d '-' -f 3`
      last_serial=`echo $warc_series | cut -d '-' -f 4`

      # metadata per BK, intended to be like books
      #   Subject: metadata for the web stuff going into the paired archive
      #   Date:  2010-09-18T12:16:00PDT
      scanner=`hostname -f`
      creator=`config.py $CONFIG creator`
      sponsor=`config.py $CONFIG sponsor`
      contributor=`config.py $CONFIG contributor`
      # scandate (using 14-digits of timestamp of first warc in series)
      # metadate (like books, the year)
      operator=`config.py $CONFIG operator`
      scancenter=`config.py $CONFIG scanningcenter`
      access="http://www.archive.org/details/${bucket}"
      crawler_version=`zcat ${files[0]} | head | grep software\
        | awk '{print $2}'`
      description=`config.py $CONFIG description\
        | sed -e s/CRAWLHOST/$scanner/\
          -e s/CRAWLJOB/$crawljob/\
          -e s/START_DATE/"$start_date_HR"/\
          -e s/END_DATE/"$end_date_HR"/`

      # support multiple arbitrary collections
      # webwidecrawl/collection/serial
      #   => collection3 = webwidecrawl
      #   => collection2 = collection
      #   => collection1 = serial
      COLLECTIONS=''
      colls=`config.py $CONFIG collections | tr '/' ' '`
      coll_count=`echo $colls | wc -w`
      for c in $colls
      do
          # --header 'x-archive-meta01-collection:${collection1}'\
          # --header 'x-archive-meta02-collection:${collection2}'\
          # --header 'x-archive-meta03-collection:${collection3}'\
          collection[${coll_count}]=$c
          coll_serial=`printf "%02d" $coll_count`
          COLLECTIONS="${COLLECTIONS}\
            --header 'x-archive-meta${coll_serial}-collection:${c}'"
          ((coll_count--))
      done

      # this breaks if CONFIG is not fqpn
      if [ -z "$creator" ] || [ -z "$sponsor" ] || [ -z "$contributor" ] ||\
         [ -z "$description" ] || [ -z $scancenter ] || [ -z $operator ] 
      then
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

      echo "Creating item: http://archive.org/details/${bucket}" | tee -a $OPEN

      if [ $force == 0 ]; then query_user; fi

      # 1) create new item with MANIFEST, metadata (auto-make-bucket)
      # --------------------------------------------------------------
      filepath=$MANIFEST
      filename=`echo $filepath | grep -o "[^/]*$"`".txt"
      copts="--include --location\
       --header 'x-amz-auto-make-bucket:1'\
       --header 'x-archive-queue-derive:${derive}'\
       --header 'x-archive-size-hint:${size_hint}'\
       --header \"authorization: LOW ${access_key}:${secret_key}\"\
       --header 'x-archive-meta-mediatype:${mediatype}'\
       $COLLECTIONS\
       --header \"x-archive-meta-title:${title}\"\
       --header \"x-archive-meta-description:${description}\"\
       --header 'x-archive-meta-subject:${subject}'\
       --header 'x-archive-meta-scanner:${scanner}'\
       --header \"x-archive-meta-creator:${creator}\"\
       --header 'x-archive-meta-scandate:${scandate}'\
       --header 'x-archive-meta-date:${metadate}'\
       --header \"x-archive-meta-sponsor:${sponsor}\"\
       --header \"x-archive-meta-contributor:${contributor}\"\
       --header 'x-archive-meta-scanningcenter:${scancenter}'\
       --header 'x-archive-meta-operator:${operator}'\
       --header 'x-archive-meta-identifier-access:${access}'\
       --header 'x-archive-meta-crawler:${crawler_version}'\
       --header 'x-archive-meta-crawljob:${crawljob}'\
       --header 'x-archive-meta-numwarcs:${num_warcs}'\
       --header 'x-archive-meta-sizehint:${size_hint}'\
       --header 'x-archive-meta-firstfileserial:${first_serial}'\
       --header 'x-archive-meta-firstfiledate:${first_file_date}'\
       --header 'x-archive-meta-lastfileserial:${last_serial}'\
       --header 'x-archive-meta-lastfiledate:${last_file_date}'\
       --header 'x-archive-meta-lastdate:${last_date}'\
       --write-out '%{http_code} %{size_upload} %{time_total}'\
       --upload-file ${filepath}"
      retry_count=0
      keep_trying='true'
      upload_type="auto-make-bucket"
      bucket_status=0
      until [ $keep_trying == 'false' ] # RETRY loop
      do
	  echo "-----" | tee -a $OPEN
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
          abort_series=0
	  bad_etag=0
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
          checksum=`grep $filename $MANIFEST | awk '{print $1}'`
          download="http://${dl}/${bucket}/${filename}"
          tombstone="${filepath}.tombstone"
          retry_count=0
          keep_trying='true'
          upload_type="add-to-bucket"
          until [ $keep_trying == 'false' ] # RETRY loop
          do
              echo "----" | tee -a $OPEN
              echo "["$((${i}+1))"/${#files[@]}]: ${filename}" | tee -a $OPEN
              copts="--include --location\
                     --header \"authorization: LOW ${access_key}:${secret_key}\"\
                     --header \"Content-MD5: ${checksum}\"\
                     --header 'x-archive-queue-derive:${derive}'\
                     --write-out '%{http_code} %{size_upload} %{time_total}'\
                     --upload-file ${filepath}"
              curl_s3
	      if [ $abort_series -eq 1 ]
              then
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

else
  echo "Usage:" `basename $0` $usage
  exit 1
fi

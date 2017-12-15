"""
Python re-implementation of s3-launch-transfers.sh
"""

import sys
import os
import re
from datetime import datetime
from calendar import timegm
import time
import itertools
import logging

from .steps import DrainStep
from .config import DrainConfig

class TransferStep(DrainStep):
    DESCRIPTION = """Upload files through IAS3 and verify they landed in the
    target item successfully."""

    S3HOST = 's3.us.archive.org'
    DOWNLOAD = 'https://archive.org/download'
    STD_WARC_SIZE = 1024**3 # 1 GiB

    CURL = 'curl'

    def __init__(self, config, interactive, mode):
        self.log = logging.getLogger(__name__) # TODO: change name
        self.config = config
        self.interactive = interactive
        self.mode = mode

    def download_url(self, bucket, filename):
        return "{}/{}/{}".format(self.DOWNLOAD, bucket, filename)
    def itempage_url(self, bucket):
        return "https://archive.org/details/" + bucket

    def curl_s3(self, upload_type, localfile, bucket, filename,
            retries=1, derive=False, method=None, headers=None, copts=None):
        """Perform S3 API call to upload `localfile` into `bucket` as
        `filename`, check existence, etc.

        :param localfile:
        :param bucket:
        :param filename:
        :param retries: the number of retries.
        :param derive: whether tigger derive after file upload \
        (only effective for upload)
        :param method: HTTP method
        :param headers: HTTP request headers
        :type headers: dict
        :param copts: additional options for ``curl``
        :type copts: sequence
        """

        block_delay = self.config.block_delay
        max_block_count = self.config.max.block_count

        log = self.log

        # get keys - TODO: add these properties to DrainConfig
        access_key = self.config.access_key
        secret_key = self.config.secret_key

        common_opts = [
            '--include', '--location',
            '--header', "authorization: LOW {}:{}".format(access_key, secret_key)
            ]
        curlopts = common_opts[:]
        if not derive:
            curlopts.extend(['--header', 'x-archive-queue-derive:0'])
        if method:
            curlopts.append('-X{}'.format(method))
        if headers:
            for name, value in headers.iteritems():
                curlopts.extend(('--header', '{}:{}'.format(name, value)))
        if copts:
            curlopts.extend(copts)

        for retry_count in itertools.count():
            if retry_count > 0:
                if retry_count >= max_block_count:
                    log.info("RETRY count (%d) reached max_block_count: %d",
                            retry_count, max_block_count)
                    self.schedule_retry(retry_count)
                    return False
                self.info("RETRY attempt (%d) %s", retry_count, datetime.now())
            fileurl = 'https://{}/{}/{}'.format(self.S3HOST, bucket, filename)
            curl_cmd = [self.CURL, '-vv'] + common_opts + copts + [fileurl, '--write-out', '%{http_code} %{size_upload} %{time_total}']
            # TODO: log command line

            try:
                output = subprocess.check_output(curl_cmd)
                # echo_curl_output $output
                log.info("curl finished with status: %s", 0)
                response_code, size, time_total = output.split(' ')
                if response_code == "200" or response_code == "201":
                    log.info("SUCCESS: S3 PUT succeeded with response_code: %s", response_code)
                    return True
                else:
                    log.error("S3 PUT failed with response_code: %s at %s",
                            response_code, datetime.now())
                    response_code = int(response_code)
                    if response_code == 0 or 400 <= response_code < 500 or response_code == 503:
                        log.info("BLOCK: sleep for %d seconds...", block_delay)
                        # TODO: don't sleep unless block_delay is very small
                        time.sleep(block_delay)
                        log.info("done sleeping at %s", datetime.now())
                    elif 500 <= response_code < 600:
                        self.schedule_retry(retry_count)
                        return False
                    else:
                        self.schedule_retry(retry_count)
                        return False
            except CalledProcessError as ex:
                log.error("curl failed with status: %s", curl_status)
                self.echo_curl_output(output)
                self.schedule_retry(retry_count)
                return False

    def schedule_retry(self, retry_count):
        # TODO: keep_trying = False
        retry_time = int(time.time()) + retry_delay
        self.log.info("RETRY: attempt (%d) scheduled"
                "after %d seconds: %s", retry_count, retry_delay, retry_time)
        if not d.RETRY:
            d.RETRY.write(format(retry_time))

    def time_to_retry(self, d):
        """
        :param d: Series
        :rtype: bool
        """
        with d.LAUNCH.open() as log:
            # datetime
            retry_time = d.RETRY.retry_time
            retry_time_ts = timegm(retry_time.timetuple())
            self.log.info("RETRY file exists: %s [%s]", d.RETRY, retry_time)
            now = datetime.now()
            if now < retry_time:
                self.log.info("  RETRY delay (now=%s < retry_time=%s)",
                        now, retry_time)
                return False

            self.log.info("  RETRY OK (now=%s >= retry_time=%s)",
                    now, retry_time)
            self.log.info("    moving aside RETRY file")
            try:
                # may raise OSError for permission denied, etc.
                d.RETRY.remove(save_suffix=format(retry_time_ts))
            except OSError as ex:
                self.log.error("    failed to rename %s", retry_file)
                # it is very likely we cannot work on this item.
                # skip.
                return False

            self.log.info("moving aside blocking files")
            for blocker in (d.LAUNCH, d.ERROR, d.TASK):
                if blocker:
                    blocker.remove(save_suffix=format(retry_time_ts))
            return True

    def check_manifest(self, d):
        files = []
        nfiles_found = 0
        nfiles_manifest = 0
        for item in d.MANIFEST.files():
            nfiles_manifest += 1
            if d.has_file(item.basename):
                files.append(item)
                nfiles_found += 1
            else:
                # allow for re-uploading some files after the itemdir has
                # been cleaned.
                if d.has_file(item.basename + ".tombstone"):
                    nfiles_found += 1
                else:
                    info.error("file not found: %s", item.path)
                    info.error("Aborting!")
                    # cp LAUNCH.open ERROR
                    # TODO: raise an exception
                    exit(1)

        log.info("  nfiles_manifest = %s", nfiles_manifest)
        log.info("  nfiles_found = %s", nfiles_found)

        return files

    def props_from_files(self, files):
        """Extract various date metadata from filenames.
        """
        def strftime_HR(dt):
            return dt.strftime('%F %T')
        props = {}
        m = self.parse_warc_name(files[0])
        t = m['timestamp']
        dt = dateutil_parse(t)
        props = dict(
            first_serial=m.get('serial', ''),
            first_file_date=t,
            start_date_ISO=dt.isoformat(),
            start_date_HR=strftime_HR(dt),
            scandate=t[:14],
            metadate=format(dt.year),
            )
        # get dates from last file in series
        ml = self.parse_warc_name(files[-1])
        tl = ml['timestamp']
        dtl = dateutil_parse(t)
        # warc end date (from last file mtime). should (closely)
        # correspond to time of last record in series
        props.update(
            last_serial=m.get('serial', ''),
            last_date=tl,
            end_date_ISO=dtl.isoformat(),
            end_date_HR=strftime_HR(dtl)
            )
        props.update(
            date_range="{start_date_ISO} to {end_date_ISO}".format(props)
        )
        return props

    def execute(self):
        max_launch = 1 if mode == 'single' or mode == 'test' or sys.maxint
        launch_count = 0
        for d in self.list_serieses():
            # LAUNCH file is closed to transfer step.
            d.LAUNCH = StateFile(d, 'LAUNCH')
            if self.process_item(d):
                launch_count += 1
                # check mode
                if launch_count >= max_launch:
                    self.info("mode = %s, exiting normally", mode)
                    break
        self.info("%s buckets filled", launch_count)
        self.info("%s done. %s", self.name, datetime.now())

    def process_item(self, d):
        if not d.MANIFEST:
            return False
        if d.RETRY:
            if not self.time_to_retry(d):
                self.info("skipping series: %s", d.name)
                return False
        # time_to_retry is supposed to remove LAUNCH.open.
        # double check if it is in fact removed.
        if d.LAUNCH.is_open():
            return False
        if d.ERROR:
            self.warn("%s: %s file exists", d.path, d.ERROR.basename)
            return False
        if d.LAUNCH:
            return False
        if d.TASK:
            return False

        with d.LAUNCH.open() as log:
            if self.interactive:
                log.info("=== %s ===", d)
                log.info("crawldata: %s", crawldata)
                log.info("mode: %s", mode)
                log.info("  CONFIG:    %s", self.config)
                log.info("  S3CFG:     %s", self.s3cfg)
                log.info("  MANIFEST:  %s", self.MANIFEST)
                log.info("  OPEN       %s", self.LAUNCH)
                log.info("  TASK       %s", self.TASK)
                log.info("  SUCCESS    %s", self.SUCCESS)
                log.info("  TOMBSTONE: %s", self.TOMBSTONE)
                self.query_user():

            log.info("parsing MANIFEST: %s", d.MANIFEST)
            files = self.check_manifest()
            if not files:
                self.info("%s: no files to upload", d)
                return False

            first_file = files[0]
            last_file = files[-1]

            series_props = self.props_from_files(files)

            # parse config
            series_props.update(
                title_prefix=self.config['title_prefix']
                )
            block_delay = self.config['block_delay']
            max_block_count = self.config['max_block_count']
            retry_delay = self.config['retry_delay']
            derive = self.config['derive']

            # bucket metadata
            bucket = d.name
            # TODO: allow for title template
            title = "{title_prefix} {date_range}".format(series_props)

            num_warcs = len(files)
            size_hint = d.PACKED.total_size

            # web item metadata is designed so that it resembles
            # books metadata.
            metadata = dict(self.config['metadata'])
            # scandate (using 14-digits of timestamp of first warc in series)
            # metadate (like books, the year)
            # TODO: this is only applicable to WARC files
            crawler_version = self.warc_software(first_file)
            # backward compatible names used in `description` template
            series_props.update(
                CRAWLHOST=self.config['crawlhost'],
                CRAWLJOB=self.config['crawljob'],
                START_DATE=series_props['start_date_HR'],
                END_DATE=series_props['end_date_HR']
            )
            description = expand_template(self.config['description'],
                    series_props)

            metadata.update({
                "title": title,
                "description": description,
                "identifier-access": self.itempage_url(bucket)
                })
            # crawler specific metadata
            metadata.update({
                "crawler": crawler_version,
                "scandate": series_props['scandate'],
                "date": series_props['metadate'],
                "crawljob": crawljob,
                "numwarcs": num_warcs,
                #"sizehint": size_hint,
                "firstfileserial": series_props['first_serial'],
                "firstfiledate": first_file_date,
                "lastfileserial": series_props['last_serial'],
                "lastfiledate": last_file_date,
                "lastdate": last_date
                })
            # support multiple arbitrary collections
            # webwidecrawl/collection/serial
            #   => collection3 = webwidecrawl
            #   => collection2 = collection
            #   => collection1 = serial
            colls = self.config.collections
            # collections are listed in reverse order (i.e. from
            # lower collection to higher collection)
            metadata['collection'] = list(reversed(colls))

            if self.interactive:
                for k, v in metadata.items():
                    self.info("  %s = %s", k, v)
                self.query_user()

            # 1) create new item with MANIFEST, metadata (auto-make-bucket)
            if d.BUCKET_OK:
                self.info("%s exists, skipping bucket creation", d.BUCKET_OK)
            else:
                # create an item by uploading MANIFEST
                log.info("Creating item: http://archive.org/details/%s", bucket)
                log.info("-----")
                headers = {
                    'x-amz-auto-make-bucket': 1,
                    'x-archive-size-hint': size_hint,
                }
                for k, v in metadata:
                    if isinstance(v, (list, tuple)):
                        for i, iv in enumerate(v, 1):
                            headers['x-archive-meta{:02d}-{}'.format(i, k)] = iv
                    else:
                        headers['x-archive-meta-{}'.format(k)] = v
                # TODO: pass metadata etc.
                if not self.curl_s3(copts, filename=d.MANIFEST.path, headers=headers):
                    log.info("Create (auto-make-bucket) failed: %s", bucket)
                    log.info("aborting series: %s", d.name)
                    continue
                log.info("item/bucket created successfully: %s", bucket)

            # 2) run HEAD on item URL to make sure item is ready
            # item creation request above may be sitting in the queue for a while
            if d.BUCKET_OK:
                self.info("%s exists, skipping bucket check", d.BUCKET_OK)
            else:
                log.info("Checking if bucket %s exists", bucket)
                # filename="" makes request URL "<bucket name>/
                if not self.curl_s3(bucket=bucket, filename="", method='HEAD'):
                    log.info("aborting series: %s", d.name)
                    continue
                log.info("creating file: %s", d.BUCKET_OK)
                d.BUCKET_OK.write(format(d))

            # 3) add WARCs to newly created item (upload-file)
            log.info("----")
            log.info("Uploading (%d) files with size hint: %d bytes",
                    num_warcs, size_hint)
            for i, item for enumerate(files, 1):
                filename = item.basename
                download = self.download_url(bucket, filename)
                tombstone = filename + ".tombstone"
                log.info('----\n[%d/%d]: %s', i, len(files), filename)
                if d.has_file(tombstone):
                    self.info("tombstone exists, skipping upload: %s", tombstone)
                    continue
                # turn on derive on the last file UNLESS derive is disabled
                headers = {
                    'Content-MD5': item.checksum
                }
                if not self.curl_s3(bucket=bucket, item.path,
                        derive=(i == len(files) and derive), headers=headers):
                    log.info("aborting series: %s", d.name)
                    # TODO want to abort working on this series
                    continue # 3
                if self.verify_etag():
                    log.info("writing download:")
                    log.info("  %s", download)
                    log.info("into tombstone")
                    log.info("  %s", tombstone)
                    with open(os.path.join(d.path, tombstone), "w") as f:
                        print >>f, download

            self.write_success(d)

        return True

def run(configpath, force=0, mode=None):
    from .config import DrainConfig

    config = DrainConfig(configpath)
    step = TransferStep(config, interactive=not force, mode=mode)
    return step.execute()

def main():
    import argparse

    parser = argparse.ArgumentParser(TransferStep.DESCRIPTION)
    parser.add_argument('config', help="Draintasker project configuration filename")
    parser.add_argument('force', nargs='?', type=int, default=0)
    parser.add_argument('mode', nargs='?', default=None)
    args = parser.parse_args()

    # allow for customizing how to configure S3 credentials
    return run(args.config, args.force, args.mode)

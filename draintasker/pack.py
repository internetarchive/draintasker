from __future__ import unicode_literals, print_function
import sys
import os
import re
import subprocess
from datetime import datetime
import hashlib

from .steps import DrainStep
from .drain import StateFile, IllegalStateError, Series

class PackLock(object):
    def __init__(self, dirpath):
        self.dirpath = dirpath

    def acquire(self):
        openpath = os.path.join(self.dirpath, 'PACKED.open')
        if os.path.exists(openpath):
            try:
                pid = int(open(openpath, 'r').readline().rstrip())
                try:
                    os.kill(pid, 0)
                    print("OPEN file exists: %s (PID=%d)" % (openpath, pid))
                    return False
                except OSError as ex:
                    if ex.errno == os.errno.ESRCH:
                        # no such process
                        print("Removing stale %s (PID=%d)" % (openpath, pid))
            except ValuErrorr as ex:
                print("OPEN file exists: %s (PID unknown)" % (openpath,))
                return False
        with open(openpath, 'w') as w:
            print(os.getpid(), file=w)
        return True

    def release(self):
        openpath = os.path.join(self.dirpath, 'PACKED.open')
        if os.path.isfile(openpath):
            os.remove(openpath)

class PackAction(object):
    def __init__(self, item_name, files):
        self.item_name = item_name
        self.files = files

    def execute(self):
        msize = 0
        msize = sum(os.path.getsize(wpath) for wpath in files)

        pack_info = "{} {} {}".format(item_name, len(files), msize)

        self.info("files considered for packing:")
        for i, path in enumerate(files):
            self.info("[%5s] %s", i + 1, path)

        self.info("==== %s ====", pack_info)

        itemdir = Series(self.config['xfer_dir'], item_name)
        if itemdir.exists():
            raise ItemExists(item_name)

        os.mkdir(itemdir.path)

        # move files into itemdir
        for f in files:
            item_fn = self.item_filename(os.path.basename(f))
            item_path = os.path.join(itemdir.path, item_fn)

            os.rename(f, item_path)

            with open(item_path, 'rb') as f:
                md = hashlib.md5()
                while True:
                    d = f.read(8192)
                    if not d: break
                    md.update(d)
                digest = md.hexdigest()
            with itemdir.MANIFEST.open() as w:
                w.write('{} {}\n'.format(digest, item_fn))

        # leave PACKED file
        self.info("PACKED: %s" pack_info)
        itemdir.PACKED.write(pack_info + '\n')

class FileSource(object):
    def __init__(self, srcdir, target_size, file_filter=None,
                 verify=True):
        """If `srcdir` has a set of files to fill in `target_size` bucket,
        return a list of files.
        """
        self.srcdir = srcdir
        self.target_size = target_size
        if file_filter is None:
            file_filter = lambda fn: True
        self.file_filter = file_filter

        self.verify = verify

    @property
    def finish_drain(self):
        return os.path.isfile(os.path.join(self.srcdir, 'FINISH_DRAIN'))

    def verify_file(self, path):
        if path.endswith('.gz'):
            self.info("  verifying gz: %s", w)
            try:
                subprocess.check_call(['gzip', '-t', path])
            except subprocess.CalledProcessError as ex:
                return False
        return True

    def get_files(self):
        files = [fn for fn in sorted(os.listdir(self.srcdir))
                 if self.file_filter(fn)]

        # if sum of file size is less than target_size, don't bother
        # verifying files and return empty, unless FINISH_DRAIN is set.
        if not self.finish_drain:
            total_size = 0
            for fn in files:
                total_size += os.path.getsize(os.path.join(self.srcdir, fn))
                if total_size >= self.target_size:
                    break
            else:
                self.info("too few files and FINISH_DRAIN file not found")
                return []

        total_size = 0
        mfiles = []
        for fn in files:
            path = os.path.join(self.srcdir, fn)
            if self.verify:
                if not self.verify_file(path):
                    # set aside broken files and skip.
                    # TODO: want to customize how to set aside
                    # bad gzip files?
                    self.debug("  mv %s %s.bad", w, w)
                    badpath = os.path.join(self.srcdir, w + ".bad")
                    try:
                        os.rename(path, badpath)
                    except OSError as ex:
                        self.error("failed to rename: %s", ex)
                    continue

            fsize = os.path.getsize(path)
            if total_size + fsize <= self.target_size:
                mfiles.append(path)
                total_size += fsize
                continue

            # if single file is larger than max_size, make a bundle with
            # that file.
            if len(mfiles) == 0:
                mfiles.append(path)
                total_size += fsize

            break

        return mfiles

class PackFiles(DrainStep):
    DESCRIPTION = """Collects WARC files into item directory in
    xfer_job_dir"""

    SUFFIX_RE = r'\.w?arc(\.gz)?'

    def __init__(self, config, mode, interactive,
                 filesource):
        """PackFiles is the collection step. In this step, files are
        collected from source directory as a new item.

        :param config:
        :param mode: ``single`` for processing just one item. \
        ``mluti`` for packing multiple items as long as source directory \
        has enough files for an item.
        :param interactive: ``True`` for pausing after printing \
        what's going to happen.
        """
        self.config = config
        self.mode = mode
        self.interactive = interactive

        self.filesource = filesource

        self.PACKED = PackLock(self.srcdir)

    @property
    def source(self):
        return self.config['job_dir']


    def _fnfilter(self):
        pat = re.compile(self._filename_regexp + self.SUFFIX_RE + '$')
        return pat.match is not None

    def parse_warc_name(self, basename):
        regex = (
            re.sub(r'\{([^\}\s]+?)}', r'(?P<\1>.*)', self.config.warc_name_pattern) +
            r'(?P<ext>\.w?arc(?P<gz>(?:\.gz)?))$'
        )
        m = re.match(regex, basename)
        if m:
            d = m.groupdict()
            if 'host' in d:
                d['shost'] = d['host'].split('.', 1)[0]
            if 'timestamp' in d:
                d['timestamp14'] = d['timestamp'][:14]
            return d
        else:
            return None

    def make_item_name(self, first_warc, last_warc):
        first_comps = self.parse_warc_name(first_warc)
        last_comps = self.parse_warc_name(last_warc)
        comps = dict(first_comps)
        comps.update({'last' + k: v for k, v in last_comps.items()})
        return self.config.item_name_template.format(**comps)

    # TODO filename translation shall happen during transfer.
    def item_filename(self, basename):
        """Return filename in item. It is identical if ``compact_names`` option
        is off. If it is on, compact name is generated with built-in template.

        :param basename: basename of the file
        """
        if self.config['compact_names']:
            comps = self.parse_warc_name(basename)
            basename = "{prefix}-{timestamp14}-{serial}{ext}".format(**comps)
        return basename

    def execute(self):
        if self.PACKED.acquire():
            try:
                rc = self._execute()
            finally:
                self.PACKED.release()
        else:
            rc = 0
        ts = datetime.now()
        self.info("pack done. %s", ts.strftime('%Y-%m-%d %H:%M:%S'))
        return rc

    def _execute(self):
        files = [fn for fn in sorted(os.listdir(self.source))
                 if self.parse_warc_name(fn)]

        counters = dict(
            warc_count=0,
            series_count=0,
            pack_count=0,
            valid_count=0,
            gz_ok=0,
            total_num_warcs=len(files),
            total_size_warcs=sum(os.path.getsize(
                os.path.join(self.source, fn)) for fn in files)
        )

        if not self.finish_drain:
            if counters['total_size_warcs'] < self.config['max_size']:
                self.info("too few WARCs and FINISH_DRAIN file not found, exiting normally")
                return 0

        mfiles = []
        msize = 0
        for w in files:
            wpath = os.path.join(self.source, w)

            if w.endswith('.gz'):
                if self.mode != "test" and self.config['verify_gzip']:
                    self.info("  verifying gz: %s", w)
                    try:
                        subprocess.check_call(['gzip', '-t', wpath])
                    except subprocess.CalledProcessError as ex:
                        self.error("bad gzip, skipping file: %s", w)
                        # TODO: want to customize the method of setting aside
                        # bad gzip files?
                        self.debug("  mv %s %s.bad", w, w)
                        badpath = os.path.join(self.source, w + '.bad')
                        try:
                            os.rename(wpath, badpath)
                        except OSError as ex:
                            self.error("failed to rename")
                        continue
                    counters['gz_ok'] += 1

            fsize = os.path.getsize(wpath)
            # previous code left the last warc file in the source directory
            # even when it fits the current item, if FINISH_DRAIN is not turned on.
            # not sure why it needed to do that way.
            if msize + fsize <= self.config['max_size']:
                # keep adding file until msize > max_size
                mfiles.append(wpath)
                msize += fsize
                continue

            # if single file is larger than max_size, make item with it alone.
            # otherwise, send this file to the next item.
            if len(mfiles) == 0:
                mfiles.append(wpath)
                msize += fsize
                _mfiles = []
                _msize = 0
            else:
                _mfiles = [wpath]
                _msize = fsize

            # breaks when item directory is secured successfully
            while True:
                item_name = self.make_item_name(
                    os.path.basename(mfiles[0]),
                    os.path.basename(mfiles[-1]))
                # TODO: use exception
                if item_name is None:
                    self.error('item idnetifier generation failed')
                    return 1

                pack_info = "{} {} {}".format(item_name, len(mfiles), msize)

                self.info("files considered for packing:")
                for i, path in enumerate(mfiles):
                    self.info("[%5s] %s", i + 1, path)

                self.info("==== %s ====", pack_info)

                itemdir = Series(self.config['xfer_dir'], item_name)
                if itemdir.exists():
                    # item directory exists. this happens when item_name was
                    # generated with less information than available (ex. using
                    # only DATE part of timestamp).
                    self.info("%s exists", item_dir)
                    if itemdir.PACKED:
                        if self.config['compact_names']:
                            self.warn("%s/PACKED exists - item name conflict,"
                                      " adding suffix to resolve", itemdir.path)
                            # TODO: generate suffix and retry
                        else:
                            self.error("%s/PACKED exists - item name conflict, aborting",
                                       itemdir.path)
                            # TODO: raise an exception?
                            return 1
                else:
                    os.mkdir(itemdir.path)
                    break

            # move files in to itemdir
            for f in mfiles:
                item_fn = self.item_filename(os.path.basename(f))
                item_path = os.path.join(itemdir.path, item_fn)
                self.debug("mv %s %s", f, item_path)
                if self.mode != 'test':
                    os.rename(f, item_path)
                counters['pack_count'] += 1

                # add line to MANIFEST
                with open(item_path, 'rb') as f:
                    md = hashlib.md5()
                    while True:
                        d = f.read(8192)
                        if not d: break
                        md.update(d)
                    digest = md.hexdigest()
                with itemdir.MANIFEST.open() as w:
                    w.write('%s %s\n' % (digest, item_fn))

            # leave PACKED file
            self.info("PACKED: %s", pack_info)
            itemdir.PACKED.write(pack_info + '\n')

            if self.mode == 'single' or self.mode == 'test':
                self.info("mode = %s, exiting normally.", self.mode)
                break

            # start next warc_series

            # reset item/manifest
            msize = _msize
            mfiles = _mfiles

        self.info("{total_num_warcs} warcs, {gz_ok} count gz_OK, {valid_count} validated, "
                  "{pack_count} packed, {series_count} series".format(**counters))
        return 0

def run(configpath, force, mode='single'):
    from config import DrainConfig

    config = DrainConfig(configpath)
    step = PackFiles(config, mode, not force)
    return step.execute()

def main():
    import argparse
    parser = argparse.ArgumentParser(description=PackFiles.DESCRIPTION)
    parser.add_argument('config', help='Draintasker project configuration file')
    parser.add_argument('force', nargs='?', type=int, default=0)
    parser.add_argument('mode', nargs='?', default='single')
    args = parser.parse_args()

    return run(args.config, args.force, args.mode)

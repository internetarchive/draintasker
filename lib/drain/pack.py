#!/usr/bin/python
#
import sys
import os
import re
import subprocess

from .steps import DrainStep

class PackFiles(DrainStep):
    DESCRIPTION = """Collects WARC files into item directory in
    xfer_job_dir"""

    SUFFIX_RE = r'\.w?arc(\.gz)?'

    def __init__(self, config, mode, interactive):
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

    def execute(self):
        fnfilter = re.compile(self._filename_regexp + self.SUFFIX_RE + '$')
        files = os.listdir(self.config.job_dir)

        counters = dict(gz_ok=0)

        for w in sorted(files):
            wpath = os.path.join(self.config.job_dir, w)
            m = fnfilter.match(w)
            if not m: continue

            if w.endswith('.gz'):
                if self.mode != "test" and self.config.verify_gzip:
                    self.info("  verifying gz: %s", w)
                    try:
                        subprocess.check_call(['gzip', '-t', wpath])
                    except subprocess.CalledProcessError as ex:
                        self.error("bad gzip, skipping file: %s", w)
                        # TODO: want to customize the method of setting aside
                        # bad gzip files?
                        print "  mv {0} {0}.bad".format(w)
                        badpath = os.path.join(self.config.job_dir, w + '.bad')
                        try:
                            os.rename(wpath, badpath)
                        except OSError as ex:
                            print "ERROR: failed to rename"
                        continue
                    counters['gz_ok'] += 1
            fsize = os.path.getsize()
            counters['msize'] += fsize
            counters['warc'] += 1

            # ...

if __name__ == "__main__":
    import argparse
    from config import DrainConfig
    parser = argparse.ArgumentParser(description=PackFiles.DESCRIPTION)
    parser.add_argument('config', help='Draintasker project configuration file')
    parser.add_argument('force', nargs='?', type=int, default=0)
    parser.add_argument('mode', nargs='?', default='single')
    args = parser.parse_args()

    config = DrainConfig(args.config)
    step = PackFiles(config, args.mode, not args.force)
    step.execute()

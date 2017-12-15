"""
Python version of delete-verified-warcs.sh
"""
import sys
import os
import re

from .drain import Logging, Series
from dateutil.parser import parse as dateutil_parse

class DrainStep(Logging):
    logstack = None
    def _pushlog(self, log):
        if not isinstance(self.logstack, list):
            self.logstach = []
        self.logstack.append(self.log)
        self.log = log
    def _poplog(self):
        self.log = self.logstack.pop()

    def query_user(self):
        print "Continue [Y/n]> "
        text = sys.stdin.readline()
        return text.lower() == 'y'

    def list_buckets(self, d):
        """return item directory names in ``d'' (has no path part)."""
        def is_itemdir(fn):
            return os.path.isdir(os.path.join(d, fn))
        return [fn for fn in os.listdir(d) if is_itemdir(fn)]

    list_items = list_buckets

    def list_serieses(self, d):
        """Return items in job directory ``d`` as a list of :class:`Series`
        objects."""
        return (Series(d, fn) for fn in self.list_buckets())

    def _filename_regexp(self):
        """Regular expression for collecting and extracting
        metadata from filenames. Requires `self.config`.

        :rtype: string
        """
        def fieldpat(m):
            name, sep, regex = m.group(1).partition(':')
            if not regex:
                regex = '.+'
            return '(?P<{}>{})'.format(name, regexp)
        p = re.sub(r'\{([^\}]+)\}', fieldpat, self.config.warc_name_pattern)
        return p

    def parse_warc_name(self, fn):
        """Extract components of filename into a dictionary.
        """
        regex = self._filename_regexp()
        m = re.match(regexp, os.path.basename(fn))
        d = m.groupdict()
        return d

    # methods to be implemented in subclasses
    def execute(self):
        pass
    def report(self):
        pass

from __future__ import unicode_literals, print_function
import sys
import os

class Logging(object):
    # TODO: move these methods to super-class
    def error(self, msg, *args):
        print("ERROR:", msg % args, file=sys.stderr)
    def warn(self, msg, *args):
        print(msg % args, file=sys.stderr)
    def info(self, msg, *args):
        print(msg % args, file=sys.stderr)
    def debug(self, msg, *args):
        print(msg % args, file=sys.stderr)

class IllegalStateError(Exception):
    """:class:`StateFile` is in illegal state for the action.
    """

class StateFileWriter(object):
    def __init__(self, statefile, fileobj):
        """Handy writer for :class:`StateFile`.
        """
        self.statefile = statefile
        self.fileobj = fileobj

    def __enter__(self):
        return self
    def __exit__(self, extype, ex, tb):
        self.fileobj.close()
        # when exiting by an exception, statefile will be left in "open" state.
        # this will prevent further processing depending on this state from
        # proceeding. 
        if ex is None:
            self.statefile.close()

    def error(self, msg, *args):
        print("ERROR:", msg % args, file=self.fileobj)
    def warn(self, msg, *args):
        print("WARNING:", msg % args, file=self.fileobj)
    def info(self, msg, *args):
        print(msg % args, file=self.fileobj)

    def write(self, text):
        self.fileobj.write(text)

class StateFile(object):
    def __init__(self, series, filename):
        """Draintasker uses StateFile for keeping track of
        process states, leaving logs, etc. StateFile is often
        created with ``.open`` suffix in order to _lock_ buckets, i.e.
        to prevent other processes from working on the same item
        concurrently.

        :param series:
        :type: :class:`Series`
        :param filename: name of the StateFile, relative to `series.path` \
        (can contain directory, but directory will not be created \
        automatically)
        :type filename: string
        """
        self.series = series
        self.filename = filename

    def open(self, mode="a"):
        """Move this StateFile to `open` state and returns file-like object
        for writing to it.

        :param mode: open mode. ``"a"`` by default for appending.
        """
        if self.is_open():
            raise IllegalStateError("Cannot open: {} exists.".format(self._openpath))
        if self.exists():
            os.rename(self.path, self._openpath)
        return StateFileWriter(self, open(self._openpath, mode))

    def close(self):
        """Move this StateFile to `close` state.
        """
        if not self.is_open():
            raise IllegalStateError("Cannot close: {} not open".format(self.path))
        os.rename(self._openpath, self.path)

    def write(self, text):
        """write `text` into this StateFile.
        Write is performed in one action, without creating ``.open`` files.
        Any existing content will be replaced with ``text``.

        :param text: text to be written
        """
        with open(self.path, "w") as f:
            f.write(format(text))

    def read(self):
        """Read the content of StateFile.
        """
        with open(self.path, "r") as f:
            return f.read()

    def readlines(self):
        """Read the lines of StateFile.
        """
        with open(self.path, "r") as f:
            return f.readlines()

    def remove(self, save_suffix=None):
        """If `save_suffix` is specified, file is renamed to current filename
        + ``"."`` + `save_suffix`.
        """
        if save_suffix:
            os.rename(self.path, "{}.{}".format(self.path, save_suffix))
        else:
            os.remove(self.path)

    @property
    def path(self):
        return os.path.join(self.series.path, self.filename)
    @property
    def _openpath(self):
        return os.path.join(self.series.path, self.filename + '.open')
    @property
    def basename(self):
        return self.filename

    def __nonzero__(self):
        return self.__bool__()
    def __bool__(self):
        """Returns ``True`` if this StateFile exists and not in
        `open` state.
        """
        return os.path.exists(self.path)
    def exists(self):
        return os.path.exists(self.path)
    def is_open(self):
        return os.path.exists(self.path + ".open")
    def __str__(self):
        return self.path

class RetryState(StateFile):
    @property
    def retry_time(self):
        return datetime.utcfromtimestamp(self.read())

class PackedState(StateFile):
    def _load(self):
        d = self.read()
        if d.startswith('{'):
            self.data = json.loads(d)
        else:
            fields = d.rstrip().split(' ')
            self.data = dict(iid=fields[0], num_files=fields[1],
                    total_size=fields[2])
        return self.data

    @property
    def total_size(self):
        return self._load()['total_size']

class PackManifest(StateFile):
    def _parse(self, l):
        checksum, fn = l.rstrip().split(' ', 1)
        return SeriesFile(fn, checksum)
    def files(self):
        return [self._parse(l) for l in self.readlines()]

class SeriesFlie(object):
    def __init__(self, fn, checksum):
        """Represents a file being uploaded to an item.

        :param fn: filename relative to :class:`Series`
        """
        self.fn = fn
        self.checksum = checksum

    def basename(self):
        return self.fn

    def uploaded(self):
        """Return ``True`` if this file has been successfully uploaded
        (i.e. has corresponding ``.tombstone`` file)
        """
        # TODO

    def mark_uploaded(self):
        """Mark this file as successfully uploaded
        (i.e. creates ``.tombstone`` file)
        """
        # TODO

class Series(object):
    def __init__(self, xfer, name):
        """Series represents a staging directory for uploading files to
        an petabox item.

        :param xfer: parent directory of staging directories
        :param name: item identifier
        """
        self.xfer = xfer
        self.name = name
        self.path = os.path.join(self.xfer, self.name)

        self.RETRY = RetryState(self, 'RETRY')
        self.PACKED = PackedState(self, 'PACKED')
        self.MANIFEST = PackManifest(self, 'MANIFEST')
        #self.LAUNCH = StateFile(self, 'LAUNCH')
        self.TASK = StateFile(self, 'TASK')
        self.SUCCESS = StateFile(self, 'SUCCESS')
        self.ERROR = StateFile(self, 'ERROR')
        self.TOMBSTONE = StateFile(self, 'TOMBSTONE')
        self.BUCKET_OK = StateFile(self, 'BUCKET_OK')

        try:
            st = os.stat(self.path)
            self.mtime = st.st_mtime
        except OSError:
            self.mtime = 0

    def _path(self, fn):
        return os.path.join(self.xfer, self.name, fn)

    def exists(self):
        return os.path.isdir(self.path)
    
    def has_file(self, file):
        return os.path.isfile(self._path(file))

    def read_file(self, file):
        try:
            f = open(self._path(file))
            c = f.read().rstrip()
            f.close()
            return c
        except:
            return '?'

    def check_file(self, file):
        if self.has_file(file): return file
        file_open = file+'.open'
        if self.has_file(file_open): return file_open
        return None

    @property
    def warcs(self):
        d = os.path.join(self.xfer, self.name)
        count = 0
        for fn in os.listdir(d):
            if re.match(r'.*\.w?arc(\.gz)?$', fn):
                count += 1
        return count

    @property
    def warcs_done(self):
        d = os.path.join(self.xfer, self.name)
        count = 0
        for fn in os.listdir(d):
            if re.match(r'.*\.w?arc(\.gz)?\.tombstone$', fn):
                count += 1
        return count

    @property
    def packed(self):
        return self.check_file('PACKED')
    @property
    def manifest(self):
        return self.check_file('MANIFEST')
    @property
    def launch(self):
        return self.check_file('LAUNCH')
    @property
    def task(self):
        return self.check_file('TASK')
    @property
    def tombstone(self):
        return self.check_file('TOMBSTONE')
    @property
    def success(self):
        if self.has_file('SUCCESS'):
            return 'SUCCESS'
        else:
            return None
    @property
    def error(self):
        if self.has_file('ERROR'):
            return 'ERROR'
        else:
            return None

    @property
    def retry(self):
        if self.has_file('RETRY'):
            return 'RETRY (%s)' % (self.read_file('RETRY'),)
        else:
            return None

    @property
    def status(self):
        if self.has_file('SUCCESS'):
            return 'completed'
        if not os.path.isfile(self.RETRY):
            if self.has_file('LAUNCH.open'): return 'running'
            if self.has_file('MANIFEST.open'): return 'running'
        return ''

    def retryasap(self):
        if os.path.isfile(self.SUCCESS):
            return dict(ok=0, error='completed series')
        if os.path.isfile(self.RETRY):
            # RETRY file may get deleted by s3-launch-transfer.sh
            # while running this code
            try:
                f = open(self.RETRY)
                retrytime = f.read().rstrip()
                f.close()
                if len(retrytime) > 0:
                    try:
                        os.rename(self.RETRY, '%s.%s' % (self.RETRY, retrytime))
                    except:
                        pass
            except:
                pass

            retrytime = readfile(self.RETRY)
            if retrytime != '?':
                try:
                    os.rename(self.RETRY, self.RETRY+'.'+str(retrytime))
                except:
                    pass
        try:
            self.RETRY.write("0\n")
            return dict(ok=1)
        except Exception as ex:
            return dict(ok=0, error=str(ex))

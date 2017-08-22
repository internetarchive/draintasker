"""
Python version of delete-verified-warcs.sh
"""
import sys
import os
import re

from .drain import *

class DeleteUploadedFiles(DrainStep):
    DESCRIPTION = """Deletes files successfully uploaded."""

    def __init__(self, xferdir, interactive=False):
        self.xfer_job_dir = xferdir
        self.interactive = interactive
        self.counters = dict(items_active=0, items_inactive=0, items_cleaned=0,
                             items_error=0, items_locked=0,
                             files_removed=0, files_missing=0)

    def execute(self):
        for iid in list_items(xfer_job_dir):
            SUCCESS = os.path.join(xfer_job_dir, iid, 'SUCCESS')
            MANIFEST = os.path.join(xfer_job_dir, iid, 'MANIFEST')
            CLEAN = os.path.join(xfer_job_dir, iid, 'CLEAN')

            if os.path.exists(CLEAN + '.err'):
                log.info("%s: has CLEAN.err", iid)
                continue
            if os.path.exists(CLEAN):
                counters['items_inactive'] += 1
                continue

            # s3-launch-transfer.sh creates SUCCESS file when it has successfully
            # uploaded all files. if SUCCESS file does not exist, the item is
            # still being worked on or on hold for some error.
            if not os.path.exists(SUCCESS):
                log.info("%s: no SUCCESS yet", iid)
                counters['items_active'] += 1
                continue
            if os.path.exists(CLEAN+'.open'):
                log.info("%s: has CLEAN.open", iid)
                counters['items_locked'] += 1
                continue

            if self.clean_item(d):
                # TODO try-except
                os.rename(CLEAN+'.open', CLEAN)
                counters['items_cleaned'] += 1
            else:
                os.rename(CLEAN+'.open', CLEAN+'.err')
                # TODO show the last line of CLEAN.err
                counters['items_error'] += 1

    def report(self):
        return ["%(items_cleaned)d cleaned, %(items_inactive)d inactive, "
                "%(items_active)d, %(items_error)d, "
                "%(items_locked)d" % self.counters,
                "removed %(files_removed)d files total" % self.counters
                ]

    def read_manifest(self, fn):
        files = []
        with open(fn, 'r') as f:
            for l in (ll.rstrip().split() for ll in f):
                if len(l) >= 2:
                    files.append(l[1])
        return files

    def clean_item(self, d):
        iid = os.path.basename(d)
        MANIFEST = os.path.join(iid, 'MANIFEST')
        upload_files = self.read_manifest(MANIFEST)
        if not upload_files:
            self.warn("no uploaded files in this item")
            return True
        # first double check if all files listed in MANIFEST have been
        # uploaded. if ".tombstone"'s and MANIFEST don't agree, something
        # must have gone wrong. need to call operator's attention.
        missedout = [
            w for w in upload_files
            if not os.path.exists(os.path.join(d, "%s.tombstone" % (w,)))
            ]
        if missedout:
            # TODO: raise exception
            for w in missedout:
                self.warn("%s: listed in MANIFEST, "
                          "but no .tombstone exists", w)
            self.warn("%d file(s) not uploaded while SUCCESS exists",
                      len(missedout))
            return False

        # show what's going to happen if running in interactive mode
        if interactive:
            self.info("removing %d files in %s", len(uploaded_files), d)
            if not self.query_user():
                print "Aborting."
                return False

        self.info("cleaning %s", d)
        removed = []
        missing = []
        for w in upload_files:
            path = os.path.join(d, w)
            if os.path.exists(path):
                # files without corresponding .tombstone file should not be
                # deleted. this is already checked above, but just
                # double-checking.
                t = w + ".tombstone"
                if not os.path.exists(t):
                    # TODO: should we delete SUCCESS etc. to have
                    # s3-launch-transfer re-process this item?
                    self.error("no .tombstone for %s", w)
                    return False
                with open(t, 'r') as f:
                    url = f.readline()
                self.info("removing %s uploaded to %s", w, url)
                try:
                    os.remove(path)
                    removed.append(w)
                except OSError, ex:
                    if ex.errno == os.errno.ENOENT:
                        # somebody removed it?? fine
                        missing.append(w)
                    return False
            else:
                # missing (already deleted) files are okay. this shouldn't
                # happen under normal situation, but it is sometimes necessary
                # to add/re-upload files to the item after upload is complete.
                missing.append(w)
        if missing:
            self.info("%s: removed %d files (%d already removed)",
                      iid, len(removed), len(missing))
        else:
            self.info("%s: removed %d files", iid, len(removed))
        self.counters['files_missing'] += len(missing)
        self.counters['files_removed'] += len(removed)
        return True

    def query_user(self):
        print "Continue [Y/n]> "
        text = sys.stdin.readline()
        return if text == 'Y'

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(DeleteUploadedFiles.DESCRIPTION)
    parser.add_argument('xfer_job_dir')
    parser.add_argument('force', nargs='?', type=int, default=0)
    args = parser.parse_args()

    print "%s: %s" % (sys.argv[0], datetime.now())
    step = DeleteUploadedFiles(args.xfer_job_dir, interactive=(not args.force))

    try:
        step.execute()
        # TODO report exception before "done" message
    finally:
        for report in step.report():
            print "%s: %s" % (sys.argv[0], report)
        print "%s: done %s" % (sys.argv[0], datetime.now())

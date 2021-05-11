# you need to edit this script to do anything useful.

import sys, getopt, os, traceback

from ZODB.FileStorage import FileStorage
from DirectoryStorage.utils import DirectoryStorageError
from DirectoryStorage.Full import Full
from DirectoryStorage.Filesystem import Filesystem

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vq", ['storage='])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    if len(args) != 1:
        # print help information and exit:
        sys.exit(usage())
    verbose = 0
    storage = None
    for o, a in opts:
        if o == '-v':
            verbose += 1
        elif o == '-q':
            verbose -= 1
        elif o == '--storage':
            storage = a
    try: fs2ds(storage, args[0], verbose)
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())

def fs2ds(fspath,dspath,verbose):
    if not os.path.exists(dspath):
        sys.exit('ERROR: %s not exists' % dspath)
    try:
        fs = FileStorage(fspath, read_only = 1)
        dst = Full(Filesystem(dspath))
        zodb_verbose = (verbose>=2)
        dst.copyTransactionsFrom(fs, zodb_verbose)
    finally:
        fs.close()
        dst.close()
    print >> sys.stderr, 'Imported to', dspath


def usage():
    return """Usage: %s [options] output_directory

Convert a FileStorage (Data.fs) into a DirectoryStorage.

Options are:

    --storage DIRECTORY

        The full path to the storage File (Data.fs).
""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

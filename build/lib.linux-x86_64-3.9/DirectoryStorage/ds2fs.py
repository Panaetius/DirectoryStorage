#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

from __future__ import nested_scopes

import sys, getopt, os, traceback
from ZODB.FileStorage import FileStorage
from DirectoryStorage.utils import DirectoryStorageError
from DirectoryStorage.FullSimpleIterator import FullSimpleIterator
from DirectoryStorage.snapshot import snapshot

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
    try:
        s = snapshot(storage)
        s.acquire()
        try:
            c = ds2fs(s.path, args[0], verbose)
        finally:
            s.release()
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())

def ds2fs(dspath,fspath,verbose):
    # Create an object that can get data out of a DirectoryStorage using the crazy
    # ZODB iterator interface. See that class for documentation about how it gets its
    # data
    it = FullSimpleIterator(dspath,verbose)
    #
    # Create a new FileStorage. check for existence first to avoid accidental damage
    if os.path.exists(fspath):
        sys.exit('ERROR: %s already exists' % fspath)
    fs = FileStorage(fspath)
    #
    fs.copyTransactionsFrom(it)
    #
    print >> sys.stderr, 'Imported to:', fspath



def usage():
    return """Usage: %s [options] output_data.fs

Convert a DirectoryStorage into a FileStorage (Data.fs).

Options are:

    --storage DIRECTORY

        The full path to the storage. May only be omitted if run
        under the snapshot.py tool.
""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

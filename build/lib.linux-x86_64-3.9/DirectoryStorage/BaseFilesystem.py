# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, time, sys, struct, threading
from cStringIO import StringIO
from utils import z64, z128, OMAGIC, TMAGIC, oid2str, ConfigParser
from utils import DirectoryStorageError, FileDoesNotExist
from utils import logger, loglevel_INFO

class FilesystemPrimitives:
    # Object which defines interface for convenient low-level transactional
    # file and directory operations. Some operations are low level, some
    # high level.
    #
    # Subclasses implement these operations using appropriate
    # os primitives.
    #
    # This abstraction has been designed to be efficiently implementable
    # on posix, and Windows. So far only the posix
    # variant has been implemented

    def exists(self,name):
        # Determine whether the file exists.
        # No specific transactional semantics
        raise NotImplementedError('exists')

    def isdir(self,name):
        # Determine whether the file is a directory.
        # No specific transactional semantics
        raise NotImplementedError('isdir')

    def mkdir(self,dir):
        # Create the directory. This change must be written
        # to stable storage after calling sync_directory on its parent
        raise NotImplementedError('mkdir')

    def sync_directory(self,dir):
        # Causes all changes (files created and deleted) in this directory
        # to be written to stable storage
        raise NotImplementedError('sync_directory')

    def write_file(self,filename,content):
        # Write those bytes to the specified file. If an old file
        # exists in the same name, it is overwritten. Data is written to
        # stable storage, but sync_directory must be called on its
        # parent if this was a new file
        raise NotImplementedError('write_file')

    def read_file(self,filename):
        # Return the contents of the specified file in a string
        # raises FileDoesNotExist if necessary
        raise NotImplementedError('read_file')

    def modify_file(self,filename,offset,content):
        # Write those bytes at the specified offset to the specified file.
        # Data is not immediately written to stable storage
        raise NotImplementedError('modify_file')

    def listdir(self,filename):
        # Like os.listdir, but must be scalable to many thousands
        # of files
        raise NotImplementedError('listdir')

    def rename(self,a,b):
        # Move file a to b. File b must not previously exist.
        # This operation must be atomic
        raise NotImplementedError('rename')

    def overwrite(self,a,b):
        # Move file a to b. File b may previously exist
        # This operation must be atomic
        raise NotImplementedError('overwrite')

    def unlink(self,a):
        # Remove file a.
        # raises FileDoesNotExist if necessary
        raise NotImplementedError('unlink')

    def rmdir(self,a):
        # Remove directory a.
        raise NotImplementedError('rmdir')

    def mark_context(self,base):
        # Create a new mark context. All files are initially unmarked
        raise NotImplementedError('mark_context')


class BaseMarkContext:

    def mark(self,a):
        # mark a file. this is used by the mark/sweep garbage collector during packing
        # it is not an error to mark or unmark a file that does not exist
        raise NotImplementedError('mark')

    def unmark(self,a):
        # Unmark a file. this is used by the mark/sweep garbage collector during packing
        # it is not an error to mark or unmark a file that does not exist
        raise NotImplementedError('unmark')

    # At one time there was a  maybe_unmark() method in this interface.
    # All uses of it have been declared unsafe, and the method removed.
    # http://sourceforge.net/mailarchive/forum.php?thread_id=6554638&forum_id=9987

    def is_marked(self,a):
        # Determine whether a file has been marked.
        # An old definition of this interface allowed for false positives. This
        # is no longer permitted.
        raise NotImplementedError('is_marked')


class BaseFilesystem(FilesystemPrimitives):
    # Higher level transactional filesystem operations. This defines the interface
    # that is required by DirectoryStorage.

    # Our logging log level for normal activity messages messages.
    ENGINE_NOISE = loglevel_INFO

    def __init__(self):
        if not self.exists('.'):
            raise DirectoryStorageError('Cant use %r, it doesnt exist' % (self.dirname,))
        self.config = ConfigParser()
        self.config.readfp(StringIO(self.read_file('config/settings')))
        if self.config.get('structure','version') not in ['0.11']:
            raise DirectoryStorageError('Bad version number')
        self.use_sync = self.config.getint('filesystem','sync')
        if not self.use_sync:
            logger.log(self.ENGINE_NOISE,
                       'sync disabled. Transactions are not durable.')

    def engage(self,synchronous=0):
        # Called by DirectoryStorage before using it as a storage
        self._lock()
        self.half_relock()

    def close(self):
        # called when the storage is closed
        raise NotImplementedError('close')

    def transaction(self,tid):
        # must return a BaseFilesystemTransaction instance
        raise NotImplementedError('transaction')

    def _lock(self):
        # the main lock on the directory.
        # In a change since version 1.0, it does not acquire the sub-lock
        raise NotImplementedError('_lock')

    def half_unlock(self):
        # release the sublock
        raise NotImplementedError('half_unlock')

    def half_relock(self):
        # regain the sublock
        raise NotImplementedError('half_relock')

    def read_database_file(self,name):
        # return the content of that named record
        # in a string
        raise NotImplementedError('read_database_file')


class BaseFilesystemTransaction:
    # BaseStorage maintains a commit lock that ensures that only one instance
    # will be in use at one time.

    def write(self,name,data):
        # write a named record into the database
        raise NotImplementedError('write')

    def vote(self):
        # last chance raise an exception at end of transaction
        raise NotImplementedError('vote')

    def finish(self):
        # finalize transaction
        raise NotImplementedError('finish')

    def abort(self):
        raise NotImplementedError('abort')



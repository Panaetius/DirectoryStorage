# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, struct, stat, errno, md5, Queue, time, sys, threading, weakref
import mimetools
from posix import fsync

from zc.lockfile import LockFile
from ZODB.FileStorage import FileStorage
from ZODB import POSException
from ZODB.DB import DB
from BTrees.OIBTree import OIBTree

from utils import ConfigParserError, DirectoryStorageError
from utils import z64, z128, oid2str, logger, loglevel_BLATHER
from LocalFilesystem import LocalFilesystem, LocalFilesystemTransaction, FileDoesNotExist

from readdir import opendir  # Error on this line? forgot to run compile.py?

class PosixFilesystem(LocalFilesystem):

    def __init__(self,dirname):
        LocalFilesystem.__init__(self,dirname)
        if self.use_sync:
            try:
                self._use_dirsync = self.config.getint('posix','dirsync')
                if not self._use_dirsync:
                    logger.log(self.ENGINE_NOISE,
                               'fsync suppressed for directories. '
                               'Transactions may or may not be durable.')
            except ConfigParserError:
                self._use_dirsync = 1
        else:
            self._use_dirsync = 0

    def transaction(self,tid):
        return PosixFilesystemTransaction(self,tid)

    def exists(self,name):
        return os.path.exists(os.path.join(self.dirname,name))

    def isdir(self,name):
        return os.path.isdir(os.path.join(self.dirname,name))

    def mkdir(self,dir):
        os.mkdir(os.path.join(self.dirname,dir))

    def sync_directory(self,dir):
        if self._use_dirsync:
            p = os.path.join(self.dirname,dir)
            # Use os.open here because, mysteriously, it performs better
            # than fopen on linux 2.4.18, reiserfs, glibc 2.2.4
            f = os.open(p,os.O_RDONLY)
            # Should we worry about EINTR ?
            try:
                # Get OSError: [Errno 22] Invalid argument on this next fsync?
                # On NFS? Setting posix/dirsync=0 in the configuration
                # file will avoid this exception.
                fsync(f)
            finally:
                os.close(f)

    def write_file(self,filename,content):
        fullname = os.path.join(self.dirname,filename)
        f = os.open(fullname,os.O_CREAT|os.O_RDWR|os.O_TRUNC,0640)
        # Should we worry about EINTR ?
        try:
            os.write(f,content)
            if self.use_sync:
                fsync(f)
        finally:
            os.close(f)

    def modify_file(self,filename,offset,content):
        fullname = os.path.join(self.dirname,filename)
        f = os.open(fullname,os.O_CREAT|os.O_RDWR,0640)
        try:
            os.lseek(f,offset,0)
            os.write(f,content)
        finally:
            os.close(f)

    def first_half_write_file(self,filename,content):
        fullname = os.path.join(self.dirname,filename)
        f = os.open(fullname,os.O_CREAT|os.O_RDWR|os.O_TRUNC,0640)
        os.write(f,content)
        os.close(f)
        # Waaah, in the general case we cant afford to keep the file open
        return fullname

    def second_half_write_file(self,fullname):
        if self.use_sync:
            f = os.open(fullname,os.O_RDONLY)
            try:
                fsync(f)
            finally:
                os.close(f)

    def abort_half_write_file(self,f):
        pass

    def read_file(self,filename):
        full = os.path.join(self.dirname,filename)
        while 1:
            try:
                f = os.open(full,os.O_RDONLY)
            except EnvironmentError,e:
                if e.errno == errno.EINTR:
                    # Its wierd, but it happens
                    pass
                elif e.errno == errno.ENOENT:
                    raise FileDoesNotExist('DirectoryStorage file %r does not exist' % (filename,) )
                else:
                    raise
            else:
                break
        try:
            chunks = []
            while 1:
                chunk = os.read(f,1024*16)
                if not chunk:
                    break
                chunks.append(chunk)
            c = ''.join(chunks)
        finally:
            os.close(f)
        return c

    def listdir(self,filename,skip_marks=1):
        # Python os.listdir is not scalable. What alternative should we use?
        if 0:
            # This easy version is not scalable to large directories
            l = os.listdir(os.path.join(self.dirname,filename))
            if skip_marks:
                l = [n for n in l if not n.endswith('.mark')]
            return l
        else:
            # Use our C extension
            return IncListDir(os.path.join(self.dirname,filename),skip_marks)

    def rename(self,a,b):
        os.rename(os.path.join(self.dirname,a),os.path.join(self.dirname,b))

    def overwrite(self,a,b):
        os.rename(os.path.join(self.dirname,a),os.path.join(self.dirname,b))

    def unlink(self,a):
        full = os.path.join(self.dirname,a)
        try:
            os.unlink(full)
        except EnvironmentError,e:
            if e.errno == errno.ENOENT:
                raise FileDoesNotExist('DirectoryStorage file %r does not exist' % (a,))
            else:
                raise

    def rmdir(self,a):
        os.rmdir(os.path.join(self.dirname,a))

    _lock_file = None
    _sub_lock_file = None
    def _lock(self):
        # In a change since version 1.0, it does not acquire the sub-lock
        if not self._lock_file:
            self._lock_file = LockFile(os.path.join(self.dirname, 'misc/lock'))

    def half_unlock(self):
        if self._sub_lock_file:
            self._sub_lock_file.close()
            del self._sub_lock_file

    def half_relock(self):
        if not self._sub_lock_file:
            self._sub_lock_file = LockFile(os.path.join(self.dirname, 'misc/sublock'))

    def close(self):
        LocalFilesystem.close(self)
        if self._sub_lock_file:
            self._sub_lock_file.close()
            del self._sub_lock_file
        if self._lock_file:
            self._lock_file.close()
            del self._lock_file

    def mark_context(self,base):
        s = weakref.proxy(self)
        mc = _mark_policies[self.config.get('posix','mark')](s)
        mc.unmark_all(base)
        return mc



class PosixFilesystemTransaction(LocalFilesystemTransaction):
    pass


class IncListDir:
    """A scalable equivalent of os.listdir.
    use an C extension module which wrappers opendir/readdir
    """
    def __init__(self,dir,skip_marks):
        self.readdir = opendir(dir).read
        self.skip_marks = skip_marks

    def __getitem__(self,i):
        # looks like a sequence when used in a for loop
        while 1:
            item = self.readdir()
            if not item:
                raise IndexError(i)
            elif item=='.' or item=='..':
                pass
            elif self.skip_marks and item.endswith('.mark'):
                pass
            else:
                return item

# various _XxxxxMarker classes implement different
# implementation policies for marking files (as
# used by the mark/sweep storage packer)

class _FileMarker:
    # Files are marked by creating another zero-length file in the
    # same directory, of the same name appended with '.mark'.
    def __init__(self,fs):
        self.fs = fs

    def mark(self,a):
        path = os.path.join(self.fs.dirname, a+'.mark')
        os.close(os.open(path, os.O_CREAT,0600))

    def unmark(self,a):
        try:
           self.fs.unlink(a+'.mark')
        except EnvironmentError,e:
           if e.errno==errno.ENOENT:
               pass
           else:
               raise

    def is_marked(self,a):
        path = os.path.join(self.fs.dirname, a+'.mark')
        return os.path.exists(path)

    def unmark_all(self,a):
        for file in self.fs.listdir(a,skip_marks=0):
            if self.fs._shutdown_flusher:
                raise DirectoryStorageError('unmark_all interrupted')
            path = os.path.join(a,file)
            if self.fs.isdir(path):
                self.unmark_all(path)
            else:
                if file.endswith('.mark'):
                    self.fs.unlink(path)


class _PermissionsMarker:
    # File are marked by setting one bit in their permissions.
    # Fast, but cheeky.  One big question is - which bit?
    #
    # Almost every 'strange' combination of bits has been abused by
    # some operating system for it own purpose, so we cant safely
    # use any of those.
    #
    # So that leaves us with a choice from the 'normal' combinations.
    # The best choice seems to be to set the 'user may execute' bit.
    #
    # A complication to this plan is that we want to allow the
    # adminstrator to make critical files immutable using file 
    # permissions. This prevents this class tweaking permissions.
    # The dictionary altmark records files that can not be marked
    # in the normal manner.
    #
    # Risk analysis:
    # This carries a very small risk; an attacker could smuggle some
    # malicious code into the database directory inside a pickle. The
    # attacker might be able to determine the oid and serial of this file
    # calculate the filename where it is stores, and find a way to trick
    # this process into executing his file during the brief period when
    # is set during packing. This is not an likely threat.
    
    def __init__(self,fs):
        self.fs = fs
        self.altmark = {}

    altmark_limit = 2000

    # Permissions.

    # CAUTION: these are in octal.

    _unmarked = 00640 # u=rw,g=r
    _marked   = 00740 # u=rwx,g=r
    _mask     = 00100
    _expected = 00100

    def mark(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            os.chmod(path,self._marked)
        except EnvironmentError,e:
            if e.errno == errno.EPERM:
                self.altmark[path] = 1
                if len(self.altmark)>self.altmark_limit:
                    raise DirectoryStorageError('Too many files are not owned by %d, %r' % (os.getuid(),path))
            elif e.errno == errno.ENOENT:
                # it is not an error to try to mark a file that does not exist
                pass
            else:
                raise

    def unmark(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            os.chmod(path,self._unmarked)
        except EnvironmentError,e:
            if e.errno == errno.EPERM:
                self.altmark[path] = 0
                if len(self.altmark)>self.altmark_limit:
                    raise DirectoryStorageError('Too many files are not owned by %d, %r' % (os.getuid(),path))
            elif e.errno == errno.ENOENT:
                pass
            else:
                raise

    def is_marked(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            return self.altmark[path]
        except KeyError:
            try:
                stats = os.stat(path)
            except EnvironmentError,e:
                if e.errno == errno.ENOENT:
                    return 0
                else:
                    raise
            return self.is_marked_stats(stats)

    def is_marked_stats(self,stats):
        if (stats[0]&self._mask) == self._expected:
            # it is marked in the normal way
            return 1
        return 0

    def unmark_all(self,a):
        for file in self.fs.listdir(a,skip_marks=0):
            if self.fs._shutdown_flusher:
                raise DirectoryStorageError('unmark_all interrupted')
            path = os.path.join(a,file)
            stats = os.stat(os.path.join(self.fs.dirname,path))
            if stat.S_ISDIR(stats[0]):       # optimised from if self.fs.isdir(path):
                self.unmark_all(path)
            else:
                if self.is_marked_stats(stats):
                    self.unmark(path)

class _StorageMarker:
    # FileStorageMarker and MinimalStorageMarker store the mark status in
    # an OIBTree in its own Storage. The files for that storage are deleted
    # once packing is complete. Both versions trigger ZODB memory leaks.
    # MinimalStorageMarker appears to be between 20% and 80% faster than
    # PermissionsMarker, although it also has a 20% space overhead. This
    # space overhead could be moved onto a different disk, which
    # may improve further on that 80% particularly if the main disk
    # is encrypted, or raid.

    def __init__(self,fs):
        self.fs = fs
        self.dir = os.path.join(fs.dirname,'misc','packing')
        # ensure that directory exists
        try:
            os.mkdir(self.dir)
        except EnvironmentError, e:
            pass
        # clean up the directory
        self._clean()
        # create the storage
        self.initstorage()
        # set up the db
        db = self.db = DB(self.substorage)
        db.setCacheSize(1000)
        self.conn = db.open()
        get_transaction().begin()
        root = self.conn.root()
        self.tree = root['tree'] = OIBTree()
        get_transaction().commit()
        self.empty = 1
        self.counter = 0

    substorage = None
    dir = None

    def __del__(self):
        if self.substorage is not None:
            # shutdown our nested ZODB
            get_transaction().abort()
            self.conn.close()
            # A circular reference is likely to keep the pickle cache around for too long.
            # Make it less large to reduce the scope of the problem
            self.conn.cacheMinimize()
            # Close the storage too
            self.substorage.close()
            # Wipe some instance attributes. This is hairy, but it helps with memory leaks
            self.db.__dict__.clear()
            self.conn.__dict__.clear()
            # Delete the directory
            self._clean()

    def _clean(self,base=None):
        if base is None:
            base = self.dir
        if base is None:
            return
        for f in os.listdir(base):
            full = os.path.join(base,f)
            if os.path.isdir(full):
                self._clean(full)
                try:
                    os.rmdir(full)
                except EnvironmentError:
                    pass
            else:
                try:
                    os.unlink(full)
                except EnvironmentError:
                    pass

    def commit(self):
        # Find a measure of how much memory we are using..... the number of BTree nodes in memory.
        # This is equal to the size of the ZODB cache, plus any nodes created since the last
        # commit. We cant quickly get an accurate count of new nodes, so this is estimated as
        # one new node for every 10 writes.
        memory_usage_measure = self.counter/10 + self.tree._p_jar._cache.cache_non_ghost_count
        if memory_usage_measure > 2000:
            # We have 2000 objects in memory. Write the changed ones to disk.
            get_transaction().commit()
            # discard all but the 1000 most recently accessed ones.
            self.tree._p_jar.cacheGC()
            self.counter = 0

    def mark(self,a):
        self.tree[a] = 1
        self.empty = 0
        self.counter += 1
        self.commit()
            
    def unmark(self,a):
        self.tree[a] = 0
        self.counter += 1
        self.commit()

    def is_marked(self,a):
        r = self.tree.get(a,None)
        self.commit()
        return r

    def unmark_all(self,a):
        if self.empty:
            return
        # When needed, this should be implemented by
        # creating a new storage
        raise NotImplementedError('unmark_all')


class _FileStorageMarker(_StorageMarker):

    def initstorage(self):
        # create the filestorage
        name = 'marks-%s.fs' % (mimetools.choose_boundary(),)
        self.substorage = FileStorage(os.path.join(self.dir,name))

class _MinimalStorageMarker(_StorageMarker):

    def initstorage(self):
        import Minimal
        import mkds
        name = 'marks-%s' % (mimetools.choose_boundary(),)
        path = os.path.join(self.dir,name)
        mkds.mkds(path,'Minimal',self.fs.format,sync=0,somemd5s=0)
        subfs = PosixFilesystem(path)
        subfs.ENGINE_NOISE = loglevel_BLATHER
        self.substorage = Minimal.Minimal(subfs)


class _MemoryMarker:
    def __init__(self,fs):
        self.marks = {}

    def mark(self,a):
        self.marks[a] = 1

    def unmark(self,a):
        self.marks[a] = 0

    def is_marked(self,a):
        return self.marks.get(a,0)

    def unmark_all(self,a):
        self.marks = {}


_mark_policies = {
    # The old favorite - store the mark flag inside file permissions.
    # This was the default in 1.1    
    'permissions' : _PermissionsMarker,

    # This used to be the old scalable alternative to 'permissions'.
    # It stores the mark bit as an extra zero length file.
    # Very slow. Not recomended.
    'file' : _FileMarker,

    # Use a dict in memory. This is the fastest if your storage is small,
    # but memory usage is proportional to storage size.
    'memory': _MemoryMarker,

    # An experimental option new in 1.1. Feedback on this is appreciated. 
    # Use *another* DirectoryStorage to contain a BTree containing mark bits.
    # The ZODB transaction/thread policy means this one has to work in
    # its own thread. See comments in its implementation about relative
    # advantages of this scheme.
    'Minimal' : _MinimalStorageMarker,

    # old name for 'Minimal'. do not use
    'MinimalStorage' : _MinimalStorageMarker,

    # This sucks. do not use
    # 'FileStorage' : _FileStorageMarker,
 }

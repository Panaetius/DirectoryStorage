# Copyright Enfold Systems
#
# Original port by Mark Hammond
# Based on Toby Dickenson's orginal code

# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, struct, stat, errno, md5, Queue, time, sys, threading, weakref, mimetools
import shutil
from os import fsync

from zc.lockfile import LockFile
from ZODB.FileStorage import FileStorage
from ZODB import POSException
from ZODB.DB import DB
from BTrees.OIBTree import OIBTree

from utils import z64, z128, oid2str, DirectoryStorageError, loglevel_BLATHER
from LocalFilesystem import LocalFilesystem, LocalFilesystemTransaction, FileDoesNotExist

import win32file, winerror, win32con
import mmap

class WindowsFilesystem(LocalFilesystem):
    def __init__(self, *args, **kw):
        LocalFilesystem.__init__(self, *args, **kw)
        # Windows cannot do this to folders so this
        # will never, ever work
        self.use_sync = 0

    def transaction(self,tid):
        return WindowsFilesystemTransaction(self,tid)

    def exists(self,name):
        return os.path.exists(os.path.join(self.dirname,name))

    def isdir(self,name):
        return os.path.isdir(os.path.join(self.dirname,name))

    def mkdir(self,dir):
        os.mkdir(os.path.join(self.dirname,dir))

    def sync_directory(self,dir):
        if self.use_sync:
            p = os.path.join(self.dirname,dir)
            # Use os.open here because, mysteriously, it performs better
            # than fopen on linux 2.4.18, reiserfs, glibc 2.2.4
            f = os.open(p,os.O_RDONLY)
            # Should we worry about EINTR ?
            try:
                fsync(f)
            finally:
                os.close(f)

    def _write_file_win32(self, fullname, content):
        h = win32file.CreateFile(fullname,
                                 win32con.GENERIC_WRITE,
                                 0, # share mode
                                 None, # security
                                 win32con.CREATE_ALWAYS, # disposition
                                 win32con.FILE_ATTRIBUTE_NORMAL, # flags/attributes
                                 None) # template.
        try:
            win32file.WriteFile(h, content)
        finally:
            h.Close()
        # Waaah, in the general case we cant afford to keep the file open
        return fullname

    def _write_file_mmap(self, fullname, content):
        # One sample on the web implied we may be able to do even better 
        # by opening the file using win32file.CreateFile, passing 
        # FILE_FLAG_NO_BUFFERING.  But for now, twice-as-good using
        # a mmaped file is good enough!
        f = os.open(fullname,os.O_CREAT|os.O_RDWR|os.O_TRUNC|os.O_BINARY,0640)
        try:
            m = mmap.mmap(f, len(content), access=mmap.ACCESS_WRITE)
            try:
                m.write(content)
            finally:
                m.close()
        finally:
            os.close(f)

    # The mmap version of _write_file is twice as fast as a win32 version.
    _write_file = _write_file_mmap

    def write_file(self,filename,content):
        fullname = os.path.join(self.dirname,filename)
        self._write_file(fullname, content)

    def modify_file(self,filename,offset,content):
        fullname = os.path.join(self.dirname,filename)
        f = os.open(fullname,os.O_CREAT|os.O_RDWR|os.O_BINARY,0640)
        try:
            os.lseek(f,offset,0)
            os.write(f,content)
        finally:
            os.close(f)

    def first_half_write_file(self,filename,content):
        fullname = os.path.join(self.dirname,filename)
        self._write_file(fullname, content)
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

    def read_file_mmap(self,filename):
        full = os.path.join(self.dirname,filename)
        try:
            f = os.open(full,os.O_RDONLY|os.O_BINARY)
        except EnvironmentError,e:
            if e.errno == errno.EINTR:
                # Its wierd, but it happens
                pass
            elif e.errno == errno.ENOENT:
                raise FileDoesNotExist('DirectoryStorage file %r does not exist' % (filename,) )
            else:
                raise
        
        m = mmap.mmap(f, 0, access=mmap.ACCESS_READ)
        os.close(f)
        return m[:]

    def read_file_win32(self,filename):
        full = os.path.join(self.dirname,filename)
        while 1:
            try:
                h = win32file.CreateFile(full,
                                         win32con.GENERIC_READ,
                                         0, # share mode
                                         None, # security
                                         win32con.OPEN_EXISTING, # disposition
                                         win32con.FILE_FLAG_SEQUENTIAL_SCAN, # flags/attributes
                                         None) # template.
            except win32file.error, e:
                if e[0] in [winerror.ERROR_FILE_NOT_FOUND,
                            winerror.ERROR_PATH_NOT_FOUND]:
                    raise FileDoesNotExist('DirectoryStorage file %r does not exist' % (filename,) )
                else:
                    raise
            else:
                break
        try:
            return win32file.ReadFile(h,win32file.GetFileSize(h),None)[1]
        finally:
            h.Close()

    # The mmap version of read_file is marginally faster than a win32
    # version (as opposed to writing, which is much faster)
    read_file = read_file_mmap
        
    def listdir(self,filename,skip_marks=1):
        # Use our C extension
        return IncListDir(os.path.join(self.dirname,filename),skip_marks)

    def rename(self,a,b):
        os.rename(os.path.join(self.dirname,a),os.path.join(self.dirname,b))

    def overwrite(self,a,b):
        win32file.MoveFileEx(os.path.join(self.dirname,a),
                             os.path.join(self.dirname,b),
                             win32file.MOVEFILE_REPLACE_EXISTING)

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
        try:
            os.rmdir(os.path.join(self.dirname,a))
        except os.error:
            raise
            #print "EEEK - directory not empty - nuking!"
            shutil.rmtree(os.path.join(self.dirname,a))

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
        mc = _mark_policies[self.config.get('windows','mark')](s)
        mc.unmark_all(base)
        return mc



class WindowsFilesystemTransaction(LocalFilesystemTransaction):
    pass


class IncListDir:
    """A scalable equivalent of os.listdir.
    use an C extension module which wrappers opendir/readdir
    """
    def __init__(self,dir,skip_marks):
        self.iter = win32file.FindFilesIterator(os.path.join(dir, "*"))
        self.skip_marks = skip_marks

    def __getitem__(self,i):
        # looks like a sequence when used in a for loop
        while 1:
            try:
                info = self.iter.next()
            except StopIteration:
                raise IndexError(i)

            item = info[-2]
            if item=='.' or item=='..':
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


class _AttributesMarker:
    # File are marked by setting one bit in their Windows "attribute"
    # Fast, but cheeky.  One big question is - which attribute?
    #
    # For Windows, we just use the SYSTEM flag!
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
    # is set during packing. This is not an unlikely threat.

    def __init__(self,fs):
        self.fs = fs
        self.altmark = {}

    altmark_limit = 2000

    _uid = 0 # deal with this later - win32security.LookupAccountName(win32api.GetDomainName(), win32api.GetUserName())

    def mark(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            win32file.SetFileAttributes(path, win32file.FILE_ATTRIBUTE_SYSTEM)
        except win32file.error, details:
            errno = details[0]
            if errno == winerror.ERROR_ACCESS_DENIED:
                self.altmark[path] = 1
                if len(self.altmark)>self.altmark_limit:
                    raise DirectoryStorageError('Too many files are not owned by %d, %r' % (os.getuid(),path))
            elif errno == winerror.ERROR_FILE_NOT_FOUND:
                # it is not an error to try to mark a file that does not exist
                pass
            else:
                raise

    def unmark(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            win32file.SetFileAttributes(path, 0)
        except win32file.error, details:
            errno = details[0]
            if errno == winerror.ERROR_ACCESS_DENIED:
                self.altmark[path] = 0
                if len(self.altmark)>self.altmark_limit:
                    raise DirectoryStorageError('Too many files are not owned by %d, %r' % (os.getuid(),path))
            elif e.errno == winerror.ERROR_FILE_NOT_FOUND:
                pass
            else:
                raise

    def is_marked(self,a):
        path = os.path.join(self.fs.dirname, a)
        try:
            return self.altmark[path]
        except KeyError:
            try:
                attr = win32file.GetFileAttributes(path)
            except win32file.error, details:
                if details[0] == winerror.ERROR_FILE_NOT_FOUND:
                    return 0
                else:
                    raise
            return (attr & win32file.FILE_ATTRIBUTE_SYSTEM) != 0

    def is_marked_stats(self,stats):
        if (stats[0]&self._mask) == self._expected:
            # it is marked in the normal way
            return 1
        if stats[4] != self._uid:
            # if not owned by us then the marked/unmarked status
            # can never be changed. we are required to treat it
            # as permanently marked
            return 1
        return 0

    def unmark_all(self,a):
        try:
            iter = win32file.FindFilesIterator(os.path.join(a, "*"))
        except win32file.error, details:
            if details[0] != winerror.ERROR_PATH_NOT_FOUND:
                raise
            return
        for info in iter:
            if self.fs._shutdown_flusher:
                raise DirectoryStorageError('unmark_all interrupted')
            attr = info[0]
            file = info[8]
            if file in (".", ".."):
                continue
            path = os.path.join(a,file)
            if attr & win32file.FILE_ATTRIBUTE_DIRECTORY:
                self.unmark_all(path)
            else:
                if attr & win32file.FILE_ATTRIBUTE_SYSTEM:
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
        subfs = WindowsFilesystem(path)
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
    # The old favorite - store the mark flag inside file attributes.
    # This was the default in 1.1
    'attributes' : _AttributesMarker,

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

# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, struct, stat, errno, md5, time, sys, string, threading, re

from ZODB import POSException
from ZODB.BaseStorage import BaseStorage
from ZODB.TimeStamp import TimeStamp

from utils import z64, z128, OMAGIC, TMAGIC, oid2str, timestamp2tid
from utils import DirectoryStorageError, DirectoryStorageVersionError, FileDoesNotExist
from utils import ConfigParserError, logger, loglevel_BLATHER

_some_unique_object = []

class BaseDirectoryStorage(BaseStorage):

    # There used to be a clean separation between this class and
    # its two subclasses, Full and Minimal. This has become dirty
    # because Full has much more development effort than Minimal.

    def __init__(self, filesystem, read_only=0, synchronous=0, argv=_some_unique_object):
        if argv is not _some_unique_object:
            sys.exit('ERROR: things have changed. read DirectoryStorage/doc/changes')
        self.filesystem = filesystem
        self._is_read_only = read_only
        # Tell the filesystem that it is being used inside a storage. That makes
        # it prepare its worker threads, etc
        self.filesystem.engage(synchronous)
        BaseStorage.__init__(self, filesystem.name())
        self.identity = self.filesystem.read_file('config/identity')
        classname = self.filesystem.config.get('storage','classname')
        if classname!=self.__class__.__name__:
            raise DirectoryStorageError('Wrong classname, %r!=%r' % (classname, self.__class__.__name__))
        self._oid = self.filesystem.read_database_file('x.oid')
        if len(self._oid)!=8:
            raise DirectoryStorageError('Bad stored oid')
        self._prev_serial = self.filesystem.read_database_file('x.serial')
        if len(self._prev_serial)!=8:
            raise DirectoryStorageError('Bad stored serial')
        self._ts = TimeStamp(self._prev_serial)
        try:
            self._last_pack = self.filesystem.read_database_file('x.packed')
        except FileDoesNotExist:
            self._last_pack = '\0'*8
        else:
            if len(self._last_pack)!=8:
                raise DirectoryStorageError('Bad last pack time')
        for op in ['read','write','overwrite','undolog','undo','history','pack']:
            v = self.filesystem.config.getint('md5policy',op)
            setattr(self,'_md5_'+op,v)
        self._times = [time.time()]
        self.history_timeout = self.filesystem.config.getint('storage','history_timeout')
        self.delay_delete = self.filesystem.config.getint('storage','delay_delete')
        #
        try:
            self.min_pack_time = self.filesystem.config.getint('storage','min_pack_time')
        except ConfigParserError:
            # settings files from 1.0 alpha 2 or earlier do not have this, but they do have
            # a different field with very similar meaning.
            self.min_pack_time = self.filesystem.config.getint('storage','window_size')
            logger.info('[storage]/min_pack_time is missing, '
                        'using [storage]/window_size instead')
        #
        try:
            self.check_dangling_references = self.filesystem.config.getint('storage','check_dangling_references')
        except ConfigParserError:
            # settings files from 1.0 alpha 1 or earlier do not have this
            logger.info('assuming config/settings should have '
                        '[storage]/check_dangling_references=1')
            self.check_dangling_references = 1
        #
        try:
            keep_policy = self.filesystem.config.get('storage','keep_policy')
        except ConfigParserError:
            # settings files from 1.0.x or earlier do not have this
            logger.info('assuming config/settings should have '
                        '[storage]/keep_policy=detailed')
            keep_policy = 'detailed'
        if keep_policy=='detailed':
            self.keep_ancient_transactions = 1
        elif keep_policy=='undoable':
            self.keep_ancient_transactions = 0
        else:
            logger.error('bad [storage]/keep_policy')
            self.keep_ancient_transactions = 1
        #
        try:
            keys = self.filesystem.config.options('keepclass')
        except ConfigParserError:
            # settings files from 1.0.x or earlier do not have this
            logger.info('assuming config/settings has an empty [keepclass] section')
            keys = []
        self.keepclass = {}
        for key in keys:
            v = self.filesystem.config.get('keepclass',key)
            if v=='forever':
                self.keepclass[key] = keep_forever()
            elif v.startswith('extra '):
                self.keepclass[key] = keep_extra( max(0,int(v[6:])) )
            else:
                logger.error('bad [keepclass]/%s' % (key,))

    def get_current_transaction(self):
        try:
            return self._serial # Zope 2.6, 2.7
        except AttributeError:
            return self._tid    # Zope 2.8

    def __len__(self):
        # should return the number of objects
        # measuring this needs a full scan of the database. maybe it
        # should be computed during a pack?
        return 0

    def getSize(self):
        # should return the total size in bytes
        return "not measured"

    def lastTransaction(self):
        return self._prev_serial

    def close(self):
        # Shut down the filesystem.
        if self.filesystem is None:
            logger.error('Duplicate call to close')
        else:
            logger.info('Closing')
            self.filesystem.close()
            self.filesystem = None

    def load(self,oid,version):
        stroid = oid2str(oid)
        data,serial2 = self._load_object_file(oid)
        self._check_object_file(oid,serial2,data,self._md5_read)
        pickle = data[72:]
        serial = data[64:72]
        return pickle,serial
 
    def loadEx(self,oid,version):
        assert not version
        pickle,serial = self.load(oid,version)
        return pickle,serial,version

    def _check_object_file(self,oid,serial,data,check_md5):
        # Given the body of an object file, check as much as we can. Its
        # redundant oid, its redundant serial number (if known), its
        # redundant length, and md5 checksum of the whole file
        if OMAGIC!=data[:4]:
            raise DirectoryStorageError('Bad magic number in oid %r' % (oid2str(oid),))
        l = struct.unpack('!I',data[4:8])[0]
        if l!=len(data):
            raise DirectoryStorageError('Wrong length of file for oid %r, %d, %d' % (oid2str(oid),l,len(data)))
        appoid = data[8:16]
        if oid!=appoid:
            raise DirectoryStorageError('oid mismatch %r %r' % (oid2str(oid),oid2str(appoid)))
        md5sum = data[40:56]
        serials_plus_pickle = data[56:]
        if md5sum!=z128 and check_md5:
            if md5.md5(serials_plus_pickle).digest()!=md5sum:
                raise DirectoryStorageError('Pickle checksum error reading oid %r' % (oid2str(oid),))
        appserial = serials_plus_pickle[8:16]
        if serial is not None and serial!=appserial:
            raise DirectoryStorageError('serial mismatch %r %r in oid %r' % (oid2str(appserial),oid2str(serial),oid2str(oid)))

    def _load_object_file(self,oid):
        # returns a tuple of the object file content, and the serial number if known
        # from a redundant source (such as filename, in a 'full' storage)
        raise NotImplementedError('_load')

    def _get_current_serial(self,oid):
        # return the current serial of this oid
        raise NotImplementedError('_get_current_serial')
        # could use some caching here?

    def _begin(self, tid, u, d, e):
        self.filesystem._pre_transaction()
        if len(u) > 65535:
            raise DirectoryStorageError('user name too long')
        if len(d) > 65535:
            raise DirectoryStorageError('description too long')
        if len(e) > 65535:
            raise DirectoryStorageError('too much extension data')
        if tid <= self._prev_serial:
            raise DirectoryStorageError('descending serial numbers in _begin')
        td = self._transaction_directory = self.filesystem.transaction(tid)
        td.u = str(u)
        td.d = str(d)
        td.e = str(e)
        if 0:
            # calculate transactions throughput figure
            self._times.append(time.time())
            self._times = self._times[-100:]
            per_item = (self._times[-1]-self._times[0])/(len(self._times)-1)
            print >> sys.stderr, '%.1f ms per transaction' % (1000*per_item,)


    def _make_file_body(self,oid,serial,old_serial,data,undofrom=z64):
        header = OMAGIC + \
                 struct.pack('!I',len(data)+72) + \
                 oid + \
                 undofrom + z128
        assert len(header)==40
        serials_plus_pickle = old_serial+serial+data
        # XXXX is it worth allowing the md5 checksum to be delayed
        # until the asynchronous flush too?
        if self._md5_write:
            md5sum = md5.md5(serials_plus_pickle).digest()
        else:
            md5sum = z128
        body = header + md5sum + serials_plus_pickle
        return body

    def _clear_temp(self):
        self._transaction_directory = None

    def _vote(self):
        td = self._transaction_directory
        self._vote_impl()
        # write out the database's most recent oid
        td.write('x.oid',self._oid)
        td.write('x.serial',self.get_current_transaction())
        # write to stable storage in the journal
        td.vote()

    def _vote_impl(self):
        # record any transaction-specific files
        raise NotImplementedError('_vote_impl')

    def _finish(self, tid, user, desc, ext):
        self._prev_serial = self.get_current_transaction()
        self._transaction_directory.finish()

    def _abort(self):
        self._transaction_directory.abort()

    def getExtensionMethods(self):
        return {'enter_snapshot': None,
                'leave_snapshot': None,
                'get_snapshot_code': None,
                'is_directory_storage': None,
                }

    def is_directory_storage(self):
        return 1

    def enter_snapshot(self,code):
        # A user process wants to enter snapshot mode. For backup, perhaps.
        if 0:
            # In versions before 1.1.4:
            #   The commit lock is acquired briefly to be sure that no
            #   transactions are partially committed at the point we enter
            #   snapshot mode. This is important in case someone is using
            #   last-modified time to determine which files were modified
            #   between subsequent snapshots, perhaps for an incremental
            #   backup. All files are either included in this snapshot, or
            #   will have an mtime after now in all subsequent snapshots.
            # Since 1.1.4 this has been inhibited because it can cause
            # deadlocks inside ZEO. The invariant described above
            # is no longer true.
            self._commit_lock_acquire()
            self._commit_lock_release()
        self.filesystem.enter_snapshot('user/'+code)
        # Release the sublock. The user process which caused us to enter
        # snapshot mode should lock the file if it needs to be sure
        # that it can hold on to snapshot mode
        self.filesystem.half_unlock()

    def leave_snapshot(self,code):
        # Re-lock the sublock file. This proves that the user process has
        # definitely finished meddling with our files
        self.filesystem.half_relock()
        # leave snapshot mode
        return self.filesystem.leave_snapshot('user/'+code)

    def get_snapshot_code(self):
        return self.filesystem.snapshot_code

    _do_packing_in_new_thread = 1 # changed by unit tests only
    def pack(self,t,referencesf):
        # Enter snapshot mode. This means that 'A' directory is a self-consistent
        # snapshot of the database with all preceeding transactions flushed
        # from the journal. The filesystem class is no longer writing to this
        # directory, therefore we can trawl through the files to determine
        # which ones are now redundant without further concerns about
        # concurrency, or journalling.
        self.filesystem.enter_snapshot('packing')
        # Note that this means only one thread can be packing at one time. We
        # dont need a separate lock, unlike other storages
        try:
            if self._do_packing_in_new_thread:
                # Do all packing work in a seperate thread so that
                # we can use a seperate ZODB to store mark information.
                # This packing mode is rarely used, but it is easier to start
                # a new thread anyway.
                mydict = {'referencesf':referencesf, 't':t}
                thread = threading.Thread(target=self._pack_thread,args=(mydict,))
                thread.start()
                thread.join()
                if mydict.has_key('exc_info'):
                    # re-raise the exception from the other thread
                    raise mydict['exc_info'][0],mydict['exc_info'][1],mydict['exc_info'][2]
                else:
                    return mydict['r']
            else:
                return self._pack_impl(referencesf, t)
        finally:
            self.filesystem.leave_snapshot('packing')

    def _pack_thread(self,mydict):
        try:
            r = self._pack_impl(mydict['referencesf'],mydict['t'])
        except:
            mydict['exc_info'] = sys.exc_info()
        else:
            mydict['r'] = r

    def _pack_impl(self,referencesf,t):
        # We have our requested pack threshold time. There are several reasons why
        # we want to adjust that threshold lower (to keep more history) but we never
        # adjust it higher (to keep less)
        upper_limit = time.time()-self.min_pack_time
        if t>upper_limit:
            # It is too close to 'now'. The configuration file can force us
            # to keep a specified amount of history.
            logger.log(loglevel_BLATHER, 'pack time threshold moved back '
                       'by %d seconds' % (t-upper_limit))
            t = upper_limit
        t = timestamp2tid(t)
        if t > self._prev_serial and self.min_pack_time>0:
            # Dont allow the pack time to be later that the most recent
            # transaction. This avoids problems for code such as incremental backups
            # and replication that uses 'the most recent transaction' as a datum, and
            # will fail if it is earlier than the pack time.
            # If the min pack time is zero then we certainly dont care about replication
            # or backup. We are probably inside a ZODB unit test, which assumes
            # this safety precaution does not exist. inhibit it            
            t = self._prev_serial
            logger.log(loglevel_BLATHER, 'pack time threshold moved back to '
                       'date of last write transaction')
        if t > self._last_pack:
            # If this pack time is later than the current pack time,
            # then remember it. This is important because files earlier than
            # this time might have been removed by packing. If later or equal to
            # this time, a missing file indicates an error.
            self._last_pack = t
            # This is a hack... we need to store the new database pack time into
            # the database directory. The right way to do that is in a transaction,
            # but that is complicated. For now just inject it directly into the
            # directory.
            self.filesystem.write_file('A/'+self.filesystem.filename_munge('x.packed'),t)
        else:
            # This pack time is earlier than a previous pack. Some storages
            # consider this an error. we dont.
            pass
        # do the packing
        return self._pack(t,referencesf)

    def _pack(self,t,referencesf):
        raise NotImplementedError('_pack')


# These two classes are used to keep some classes around longer than
# might normally be expected during pack.
        
class keep_forever:
    # Never remove it on pack
    def expired(self,threshold,tid):
        return 0

class keep_extra:
    # Keep it for an extra time
    def __init__(self,days):
        self.seconds = days*24*60*60
    def expired(self,threshold,tid):
        threshold = timestamp2tid(TimeStamp(threshold).timeTime() - self.seconds)
        return tid < threshold

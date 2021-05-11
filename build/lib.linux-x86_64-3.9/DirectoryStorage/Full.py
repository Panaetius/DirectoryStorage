# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, errno, time, sys, string, struct, md5, cPickle, random, re
import cStringIO

from ZODB import POSException
from ZODB import TimeStamp
from ZODB.ConflictResolution import ConflictResolvingStorage, ResolvedSerial

from BaseDirectoryStorage import BaseDirectoryStorage

from utils import z16, z64, z128, OMAGIC, TMAGIC, CMAGIC, oid2str, timestamp2tid
from utils import DirectoryStorageError, DirectoryStorageVersionError, FileDoesNotExist
from utils import DanglingReferenceError, POSGeorgeBaileyKeyError
from utils import class_name_from_pickle
from utils import ZODB_referencesf, logger

class Full(BaseDirectoryStorage,ConflictResolvingStorage):

    def _load_object_file(self,oid):
        serial = self._get_current_serial(oid)
        if serial is None:
            raise POSException.POSKeyError(oid)
        if len(serial)!=8:
            raise DirectoryStorageError('Bad current revision for oid %r' % (stroid,))
        data = self.filesystem.read_database_file('o'+oid2str(oid)+'.'+oid2str(serial))
        if len(data)==72:
            # This object contains a zero length pickle. that means the objects creation was undone.
            raise POSGeorgeBaileyKeyError(oid)
        return data, serial

    def _get_current_serial(self,oid):
        # could use some caching here?
        stroid = oid2str(oid)
        try:
            data = self.filesystem.read_database_file('o'+stroid+'.c')
        except FileDoesNotExist:
            return None
        return _fix_serial(data,oid)

    def _begin(self, tid, u, d, e):
        # We override this to add our own attributes to the transaction object
        BaseDirectoryStorage._begin(self,tid,u,d,e)
        td = self._transaction_directory
        td.oids = {}
        td.refoids = {}

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError('Can not store to a read-only DirectoryStorage')
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise DirectoryStorageVersionError('Versions are not supported')
        conflictresolved = 0
        old_serial = self._get_current_serial(oid)
        if old_serial is None:
            # no previous revision of this object
            old_serial = z64
        elif old_serial!=serial:
            # The object exists in the database, but the serial number
            # given in the call is not the same as the last stored serial
            # number.  First, attempt application level conflict
            # resolution, and if that fails, raise a ConflictError.
            data = self.tryToResolveConflict(oid, old_serial, serial, data)
            if data:
                conflictresolved = 1
            else:
                raise POSException.ConflictError(serials=(old_serial, serial))

        tid = self.get_current_transaction()
        assert len(tid)==8
        body = self._make_file_body(oid,tid,old_serial,data)
        refoids = []
        if self.check_dangling_references:
            ZODB_referencesf(data,refoids)
        self._write_object_file(oid,tid,body,refoids)
        if conflictresolved:
            return ResolvedSerial
        else:
            return tid

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        # A lot like store() but without all the consistency checks.  This
        # should only be used when we /know/ the data is good, hence the
        # method name.  While the signature looks like store() there are some
        # differences:
        #
        # - serial is the serial number of /this/ revision, not of the
        #   previous revision.  It is used instead of self._serial, which is
        #   ignored.
        #
        # - Nothing is returned
        #
        # - data can be None, which indicates a George Bailey object
        #   (i.e. one who's creation has been transactionally undone).
        #
        # - prev_txn is a hint that an identical pickle has been stored 
        #   for the same oid in a previous transaction. Some other storages
        #   use this to enable a space-saving optimisation. We dont.
        #
        if self._is_read_only:
            raise POSException.ReadOnlyError('Can not restore to a read-only DirectoryStorage')
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            # should this be silently ignored?
            raise DirectoryStorageVersionError('Versions are not supported')
        old_serial = self._get_current_serial(oid)
        if old_serial is None:
            # no previous revision of this object
            old_serial = z64
        if data is None:
            data = ''
        if oid>self._oid:
            self._oid = oid
        body = self._make_file_body(oid,serial,old_serial,data)
        self._write_object_file(oid,serial,body)

    def _write_object_file(self,oid,newserial,body,refoids=None):
        td = self._transaction_directory
        # refoids is a list of oids referenced by this object, which should be
        # checked for dangling references at transaction commit. If the refoids
        # parameter is not provided then we do not check any references
        if refoids:
            for refoid in refoids:
                td.refoids[refoid] = oid
        # td.oids is our primary index of objects modified in this transaction.
        # values in this mapping indicate whether the modified object is George Bailey
        is_george_bailey_revision = len(body)==72
        td.oids[oid] = is_george_bailey_revision
        stroid = oid2str(oid)
        if body:
            td.write('o'+stroid+'.'+oid2str(newserial),body)
        td.write('o'+stroid+'.c',newserial)

    def _vote_impl(self):
        # Also need to write file describing this transaction
        td = self._transaction_directory
        assert td is not None
        # Verify that every object reference corresponds to a real
        # object that already exists in the database, or one that
        # is written in this transaction. The goal is to guarantee
        # that the storage contains no dangling references. Currently
        # there are still several ways that a dangling reference can
        # be created without detection:
        # 1. Writing a George Bailey object revision when another object
        #    contains a reference to it. We only check for references
        #    in objects written in this transaction. 
        # 2. A concurrent pack may have scheduled a referenced object
        #    for removal. It is not dangling now, but it would be
        #    once the pack is complete

        good_old_oids = {}
        for refoid,soid in td.refoids.items():
            if td.oids.has_key(refoid):
                if td.oids[refoid]:
                    # A reference to a George Bailey object written in this
                    # transaction. 
                    raise DanglingReferenceError(soid,refoid)
                else:
                    # A reference to an ordinary object written in this
                    # transaction
                    pass
            elif good_old_oids.has_key(refoid):
                # We have already checked that it exists in the database
                pass
            else:
                # An object outside of this transaction. Try to load it.
                try:
                    self._load_object_file(refoid)
                except POSException.POSKeyError: 
                    # Failed to load the object.
                    raise DanglingReferenceError(soid,refoid)
                else:
                    # This object already exists in the database.
                    good_old_oids[refoid] = 1

        # Record the oid of every modified object in the transaction file
        ob = string.join(td.oids.keys(),'')
                    
        u,d,e = td.u,td.d,td.e
        assert self._prev_serial<self.get_current_transaction()
        body = struct.pack("!HHHIH",len(u),len(d),len(e),len(ob),0) + u + d + e + ob
        if self._md5_write:
            md5sum = md5.md5(body).digest()
        else:
            md5sum = z128

        header = TMAGIC + \
                 struct.pack('!I',len(body)+48) + \
                 td.tid + \
                 z64 + \
                 self._prev_serial
        # The transaction file name has a dot in the middle of it as a clue to
        # the 'bushy' format that the trailing characters are not worth dividing into
        # subdirectories
        self._transaction_directory.write(_tid_filename(td.tid), header + md5sum + body)

    def supportsTransactionalUndo(self):
        return 1

    def undoLog(self,first=0,last=-20,filter=None):
        if last < 0:
            last = first - last + 1
        i = 0
        r = []
        if self.history_timeout>0:
            timeout = time.time()+self.history_timeout
        else:
            timeout = None
        tid = self.filesystem.read_database_file('x.serial')
        while i<last:
            strtid = oid2str(tid)
            try:
                data = self.filesystem.read_database_file(_tid_filename(tid))
            except FileDoesNotExist:
                if tid>=self._last_pack:
                    # missing file
                    raise
                else:
                    # earlier transactions have been lost to packing
                    break
            self._check_transaction_file(tid,data,self._md5_undolog)
            lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
            d = { 'user_name'   : data[60:60+lenu],
                  'time'        : TimeStamp.TimeStamp(tid).timeTime(),
                  'description' : data[60+lenu:60+lenu+lend],
                  'id'           : tid }
            if lene:
                try:
                    e = cPickle.loads(data[60+lenu+lend:60+lenu+lend+lene])
                    d.update(e)
                except:
                    pass
            # this transaction data lists all the objects modified in this transaction.
            # we must check whether they all have an earlier revision to load state from
            is_undoable = 1
            oidblock = data[60+lenu+lend+lene:60+lenu+lend+lene+leno]
            assert 0==(len(oidblock)%8)
            while oidblock:
                if timeout is not None and time.time()>timeout:
                   # We have spent too long processing this request.
                   is_undoable = 0
                   break
                # oids are packed into the oidblock. no duplicates.
                oid,oidblock = oidblock[:8],oidblock[8:]
                stroid = oid2str(oid)
                # load the revision to be undone.
                try:
                    odata = self.filesystem.read_database_file('o'+stroid+'.'+strtid)
                except FileDoesNotExist:
                    if tid>=self._last_pack:
                        # missing file
                        raise
                    else:
                        # This file does not exist because it has been removed in a previous pack
                        is_undoable = 0
                        break
                self._check_object_file(oid,tid,odata,self._md5_undo)
                # We can only undo this transaction if the previous revision of the object
                # was not removed by packing.
                prevtid = odata[56:64]
                if prevtid==z64:
                    # the object was created in this transaction. Thats fine
                    pass
                else:
                    # Try to load the revision that will become the new current revision
                    # if this transactions is undone. Does the storage API require this check?
                    strprevtid = oid2str(prevtid)
                    try:
                        podata = self.filesystem.read_database_file('o'+stroid+'.'+strprevtid)
                    except FileDoesNotExist:
                        if prevtid>=self._last_pack:
                            # missing file
                            raise
                        else:
                            # The previous revision has been removed by packing
                            is_undoable = 0
                            break
                    self._check_object_file(oid,prevtid,podata,self._md5_undo)

            if is_undoable:
                if filter is None or filter(d):
                    if i >= first:
                        r.append(d)
                    i += 1
            tid = data[24:32]
            if tid==z64:
                # this was the first revision ever
                break
            if timeout is not None and time.time()>timeout:
               # We have spent too long processing this request.
               break
        return r

    def _check_transaction_file(self,tid,data,check_md5):
        strtid = oid2str(tid)
        if TMAGIC!=data[:4]:
            raise DirectoryStorageError('Bad magic number in transaction id %r' % (strtid,))
        apptid = data[8:16]
        if tid!=apptid:
            raise DirectoryStorageError('tid mismatch %r %r' % (strtid,str2oid(apptid)))
        l = struct.unpack('!I',data[4:8])[0]
        if l!=len(data):
            raise DirectoryStorageError('Wrong length of file for tid %r, %d, %d' % (strtid,l,len(data)))
        md5sum = data[32:48]
        vdata = data[48:]
        if md5sum!=z128 and check_md5:
            if md5.md5(vdata).digest()!=md5sum:
                raise DirectoryStorageError('Pickle checksum error reading oid %r' % (stroid,))

    def transactionalUndo(self, transaction_id, transaction):
        # Note that there may be a pack running concurrently.
        if self._is_read_only:
            raise POSException.ReadOnlyError('Can not undo in a read-only DirectoryStorage')
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        transaction_id = str(transaction_id)
        if len(transaction_id)!=8:
            raise DirectoryStorageError('Bad transaction_id')
        # A mapping from oid to the serial that it has been undone back to.
        td = self._transaction_directory
        if not hasattr(td,'undone'):
            td.undone={}
        # Load the transaction file so that we can find the list of oids modified
        # in this transaction
        strtid = oid2str(transaction_id)
        try:
            data = self.filesystem.read_database_file(_tid_filename(transaction_id))
        except FileDoesNotExist:
            raise POSException.UndoError('No record of that transaction')
        lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
        oidblock = data[60+lenu+lend+lene:60+lenu+lend+lene+leno]
        assert 0==(len(oidblock)%8)
        oids = {}
        this_transaction = self.get_current_transaction()
        while oidblock:
            # oids are packed into the oidblock. no duplicates.
            oid,oidblock = oidblock[:8],oidblock[8:]
            assert not oids.has_key(oid)
            oids[oid] = 1
            stroid = oid2str(oid)
            # load the revision to be undone
            data = self.filesystem.read_database_file('o'+stroid+'.'+strtid)
            self._check_object_file(oid,transaction_id,data,self._md5_undo)
            # check if this object is eligible for undo
            current = self._get_current_serial(oid)
            undocurrent = td.undone.get(oid,current)
            if undocurrent!=transaction_id:
                # The current revision is not the transaction being undone.
                # But maybe the current revision contains a *copy* of the revision being
                # undone, made during a previous undo operation...
                cdata = self.filesystem.read_database_file('o'+stroid+'.'+oid2str(undocurrent))
                self._check_object_file(oid,undocurrent,cdata,self._md5_undo)
                if cdata[16:24]!=transaction_id:
                    raise POSException.UndoError('Some objects modified by later transaction')
                # XXXX we probably should check for a copy of a copy of a copy of a copy.
            prevtid = data[56:64]
            if prevtid==z64:
                # The object was created in this transaction.
                body = self._make_file_body(oid,this_transaction,current,'',undofrom=prevtid)
                self._write_object_file(oid,this_transaction,body)
            else:
                # load the revision that will become the new current revision
                strprevtid = oid2str(prevtid)
                data = self.filesystem.read_database_file('o'+stroid+'.'+strprevtid)
                self._check_object_file(oid,prevtid,data,self._md5_undo)
                td.undone[oid] = prevtid
                # compute a new file
                body = self._make_file_body(oid,this_transaction,current,data[72:],undofrom=prevtid)
                self._write_object_file(oid,this_transaction,body)
        return oids.keys()

    def undo(self, transaction_id, transaction):
        return self.get_current_transaction(), self.transactionalUndo(transaction_id,transaction)

    def loadSerial(self, oid, serial):
        try:
            data = self.filesystem.read_database_file('o'+oid2str(oid)+'.'+oid2str(serial))
        except FileDoesNotExist:
            raise POSException.POSKeyError(oid)
        self._check_object_file(oid,serial,data,self._md5_read)
        pickle = data[72:]
        if not pickle:
            # creation was undone
            raise POSException.POSKeyError(oid)
        return pickle

    def history(self,oid,version=None,size=1,filter=None):
        assert not version
        history = []
        stroid = oid2str(oid)
        tid = self._get_current_serial(oid)
        if tid is None:
            # history of object with no current revision
            raise POSException.POSKeyError(oid)
        first = 1
        if self.history_timeout>0:
            timeout = time.time()+self.history_timeout
        else:
            timeout = None
        while len(history)<size:
            strtid = oid2str(tid)
            # Some basic information we know before we start
            d = { 'time'         : TimeStamp.TimeStamp(tid).timeTime(),
                  'serial'       : tid, # used in Zope 2.6, 2.7
                  'tid'          : tid, # used in Zope 2.8
                  'version'      : '' }
            # First load the transaction file to get most of the information
            try:
                data = self.filesystem.read_database_file(_tid_filename(tid))
            except FileDoesNotExist:
                if tid>=self._last_pack:
                    # missing file
                    raise
                else:
                    # Transaction file removed by packing.
                    # The object file may not exist either, but that will
                    # be detected further down. This happens often when using
                    # keep_policy=undoable, and possibly in other cases
                    # if packing is interrupted
                    d['user_name'] = 'User Name no longer recorded'
                    d['description'] = 'Description no longer recorded'
            else:
                self._check_transaction_file(tid,data,self._md5_history)
                lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
                d['user_name'] = data[60:60+lenu]
                d['description'] = data[60+lenu:60+lenu+lend]
            # Next load the object file to get the size, and our next pointer
            try:
                data = self.filesystem.read_database_file('o'+stroid+'.'+strtid)
            except FileDoesNotExist:
                if tid>=self._last_pack or first:
                    # missing file
                    raise
                else:
                    # Object file removed by packing.
                    # The transaction file may or may not exist
                    break
            self._check_object_file(oid,tid,data,self._md5_history)
            d['size'] = len(data)-72
            if filter is None or filter(d):
                history.append(d)
            tid = data[56:64]
            if tid==z64:
                # there is no more history for this object
                break
            if timeout is not None and time.time()>timeout:
                # We have spent too long processing this request.
                break
            first = 0
        return history

    _ok_to_pack_empty_storage = 1 # set by some unit tests

    def _pack(self,t,referencesf):
        if not self._has_root():
            logger.log(self.filesystem.ENGINE_NOISE,
                       'Skipping pack of empty storage')
            # We assume it is empty if there is no root object. This assumption
            # is valid, although some ZODB unit tests break the rules.
            assert self._ok_to_pack_empty_storage
            return
        fs = self.filesystem
        # Packing uses a mark and sweep strategy.
        #
        # Pass 1
        #
        # First, create the mark context and clear any previous marks.
        logger.info('Starting to pack')
        start_time = time.time()
        logger.log(self.filesystem.ENGINE_NOISE, 'Packing pass 1 of 4')
        mc = fs.mark_context('A')
        #
        # Pass 2
        #
        # Reachable objects and transactions will be marked.
        # An object is reachable if:
        # 1. It is the root object
        # 2. it is referenced by a sufficiently recent revision
        #    of another object which is reachable. This catches
        #    most objects
        # 3. It was written in a sufficiently recent transaction.
        #    This catches a few weird boundary cases, because
        #    most objects are caught by 2.
        #
        logger.log(self.filesystem.ENGINE_NOISE, 'Packing pass 2a of 4')
        self._mark_reachable_objects(z64,t,referencesf,mc)
        logger.log(self.filesystem.ENGINE_NOISE, 'Packing pass 2b of 4')
        self._mark_recent_transactions(t,referencesf,mc)
        # Mark some admin files
        mc.mark('A/'+fs.filename_munge('x.serial'))
        mc.mark('A/'+fs.filename_munge('x.oid'))
        mc.mark('A/'+fs.filename_munge('x.packed'))
        #
        # Pass 3
        #
        # Some reachable transactions may be earlier than non-reachable ones.
        # We need to modify the back-pointers in those transactions to
        # ensure everything links up when the unreachables are removed
        logger.log(self.filesystem.ENGINE_NOISE, 'Packing pass 3 of 4')
        self._relink_reachable_transactions(mc)
        #
        # Pass 4
        #
        # unmarked files are swept away
        logger.log(self.filesystem.ENGINE_NOISE, 'Packing pass 4 of 4')
        total = self._remove_unmarked_objects(int(time.time()),mc)
        #
        #
        elapsed = time.time()-start_time
        elapsed =  '%d:%02d:%02d' % (elapsed/3600,(elapsed/60)%60,elapsed%60)
        logger.info('Packing complete, removed %d files, elapsed time %s'
                    % (total,elapsed))

    def enter_snapshot(self,code):
        # The user is allowed to do things that might confuse our file marking
        return BaseDirectoryStorage.enter_snapshot(self,code)

    def _has_root(self):
        fs = self.filesystem
        stroid = oid2str(z64)
        name = 'o'+stroid+'.c'
        name = fs.filename_munge(name)
        name = os.path.join('A',name)
        return fs.exists(name)

    def _mark_recent_transactions(self,threshold,referencesf,mc):
        fs = self.filesystem
        tid = fs.read_file('A/'+fs.filename_munge('x.serial'))
        counter = 0
        # We definitely need to keep the two most recent transaction files
        # to allow replication/backup to use the transaction file as a datum.
        # Here we keep all the object revisions in the two most recent
        # transactions too. Thats unnecessary, but safe.
        if self.min_pack_time==0:
            # If the min pack time is zero then we certainly dont care about replication
            # or backup. We are probably inside a ZODB unit test, which assumes
            # this safety precaution does not exist. inhibit it
            counter = 2
        while tid>=threshold or counter<2:
            counter += 1
            name = _tid_filename(tid)
            name = fs.filename_munge(name)
            name = os.path.join('A',name)
            mc.mark(name)
            try:
                data = fs.read_file(name)
            except FileDoesNotExist:
                if tid>=self._last_pack:
                    raise
                else:
                    return
            self._check_transaction_file(tid,data,0)
            lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
            oidblock = data[60+lenu+lend+lene:60+lenu+lend+lene+leno]
            assert 0==(len(oidblock)%8)
            while oidblock:
                oid,oidblock = oidblock[:8],oidblock[8:]
                # Most of these objects will already be marked as reachable. The only exceptions
                # are objects that are not reachable from the root - which only happens under
                # a few strange boundary conditions. (subtransactions, cross-transation
                # references, etc). Originally DirectoryStorage removed these objects when
                # packing. However this causes some minor complications for backup and
                # replication, so it is easier to keep them.
                if self._get_current_serial(oid) is None:
                    # Missing file. This is acceptable
                    pass
                else:
                    # If we have the file, then mark it and everything that it references
                    self._mark_reachable_objects(oid,threshold,referencesf,mc)
            tid = data[24:32]
            if tid==z64:
                # back to the beginning of history
                break

    def _mark_reachable_objects(self,oid,threshold,referencesf,mc):
        todo = {oid:None}
        while todo:
            # The current ZODB oids assignment policy means that chosing the
            # largest oid in the todo list leads to keeping a small todo list
            oid = max(todo.keys())
            new = self._mark_reachable_objects_impl(oid,threshold,referencesf,mc)
            todo.update(new)
            del todo[oid]

    def _mark_reachable_objects_impl(self,oid,threshold,referencesf,mc):
        fs = self.filesystem
        # First mark the object current revision pointer file
        stroid = oid2str(oid)
        name = 'o'+stroid+'.c'
        name = fs.filename_munge(name)
        name = os.path.join('A',name)
        if mc.is_marked(name):
            # This object has already been marked,
            # so there is nothing more to do.
            return {}
        current = _fix_serial(fs.read_file(name),oid)
        mc.mark(name)
        # Next, check the files containing recent revisions of this object
        # to determine the set of referenced objects
        tid = current
        class_name = None
        keepclass = None
        allrefoids = {}
        first = 1
        while 1:
            # Load this revision
            strtid = oid2str(tid)
            __traceback_info__ = stroid,strtid
            name = 'o'+stroid+'.'+strtid
            name = fs.filename_munge(name)
            name = os.path.join('A',name)
            try:
                data = fs.read_file(name)
            except FileDoesNotExist:
                if tid>=self._last_pack or first:
                    # Missing file. This indicates database corruption.
                    # It would be dangerous to continue from here because it may lead us to
                    # think that some objects are unreachable, because they are only reachable
                    # from this missing file. Continuing the pack could make things worse.
                    raise
                else:
                    # Revision does not exist. It must have been removed by packing
                    break
            self._check_object_file(oid,tid,data,self._md5_pack)
            pickle = data[72:]
            if len(pickle)==0:
                # an object whose creation has been undone.
                # This revision references nothing
                pass
            else:
                # Record the referenced objects in the to-do list
                try:
                    refoids = []
                    referencesf(pickle,refoids)
                    for refoid in refoids:
                        allrefoids[refoid] = 1
                except (IOError,ValueError,EOFError),e:
                    if first:
                        # The current revision of an object can not be unpickled. Thats bad
                        # maybe you could undo the last transaction, and hope the second-to-last
                        # one can be unpickled?
                        logger.critical(
                            'Failure to unpickle current revision of an object')
                    else:
                        # An old revision of an object can not be unpickled. Thats ok as long
                        # as you dont want to undo. Packing with a different time would remove it.
                        timestamp = TimeStamp.TimeStamp(tid).timeTime()
                        ago = int((time.time()-timestamp)/(60*60*24))
                        logger.error(
                            'Failure to unpickle old revision of an object. '
                            'You could remove it with a pack that removes '
                            'revisions that are %d days old.' % (ago,))
                    raise
                if class_name is None:
                    class_name = class_name_from_pickle(pickle)
                    keepclass = self.keepclass.get(class_name)
            # Mark this file
            mc.mark(name)
            if tid>=threshold or self.keep_ancient_transactions:
                # Mark the corresponding transaction file
                name = _tid_filename(tid)
                name = fs.filename_munge(name)
                name = os.path.join('A',name)
                mc.mark(name)
            # check the previous revision of this object
            tid = data[56:64]
            if tid<threshold:
                # that revision is looking a little old.
                if keepclass is None or keepclass.expired(threshold,tid):
                    # It will be discarded
                    break
                else:
                    # It normally would be discarded, but we have special instructions
                    # to keep it longer than normal.
                    #logger.info('keeping a %s' % class_name)
                    pass
            first = 0
        return allrefoids

    def _relink_reachable_transactions(self,mc):
        # Packing will retain all transactions after the threshold date, but
        # only those transactions before the threshold date which still contain
        # the most recent revision of an object. It will delete all Transactions
        # which occurred earlier than the pack date, and which no longer contain
        # the most recenet revision of something.
        # Some kept transactions may be earlier than a deleted ones.
        # We need to modify the back-pointers in those transactions to
        # ensure everything links up when things are removed.
        #
        # Once we start this process there are some objects (the ones which
        # will be deleted later in the pack process) which refer to a transaction
        # which is not linked from the current transaction. All of these changes
        # are not made through the journal, so any failure between now and the end
        # of packing may mean we are in this state for a long time. I suspect this
        # could cause a problem, but I cant think of any right now.
        #
        fs = self.filesystem
        tid = fs.read_file('A/'+fs.filename_munge('x.serial'))
        prev_name = ''
        prev_ptr = ''
        while 1:
            strtid = oid2str(tid)
            name = _tid_filename(tid)
            name = fs.filename_munge(name)
            name = os.path.join('A',name)
            try:
                data = fs.read_file(name)
            except FileDoesNotExist:
                if tid>=self._last_pack:
                    raise
                else:
                    return
            self._check_transaction_file(tid,data,0)
            if mc.is_marked(name):
                # This transaction is reachable
                # Ensure it is back-linked from the previous reachable transaction
                if prev_ptr and prev_ptr!=tid:
                    fs.modify_file(prev_name,24,tid)
                # Record this... we may have to patch this file if an intermediate
                # transaction is not reachable.
                prev_name = name
                prev_ptr = data[24:32]
            else:
                pass
            tid = data[24:32]
            if tid==z64:
                # back to the beginning of history
                break

    _pointer_file_re = re.compile('^o[A-F0-9]{16}.c$')
    _object_file_re = re.compile('^o[A-F0-9]{16}.[A-F0-9]{16}$')
    _transaction_file_re = re.compile('^t[A-F0-9]{8}.[A-F0-9]{8}$')
    def _remove_unmarked_objects(self,now,mc,directory='A'):
        fs = self.filesystem
        total = 0
        empty = 1
        pretend = 0
        for file in fs.listdir(directory):
            empty = 0
            path = os.path.join(directory,file)
            if file.endswith('-deleted'):
                # this file is already awaiting delayed deletion
                try:
                    time_deleted = int(string.split(file,'-')[-2])
                except (ValueError,IndexError),e:
                    # Wierd file name. delete it
                    time_deleted = 0
                if time_deleted + self.delay_delete < now:
                    if pretend:
                        print >> sys.stderr, 'packing would delay-remove %r' % (path,)
                    else:
                        fs.unlink(path)
            else:
                if fs.isdir(path):
                    total += self._remove_unmarked_objects(now,mc,path)
                else:
                    if mc.is_marked(path):
                        if pretend:
                            print >> sys.stderr, 'packing would keep %r' % (path,)
                    else:
                        total += 1
                        if pretend:
                            print >> sys.stderr, 'packing would remove %r' % (path,)
                        else:
                            if self.delay_delete>0:
                                 fs.rename(path,path+'-'+str(now)+'-deleted')
                            else:
                                 fs.unlink(path)
        if empty:
            fs.rmdir(directory)
        return total

def _tid_filename(tid):
    return 't%02X%02X%02X.%02X%02X%02X%02X%02X' % struct.unpack('!8B', tid)

def _fix_serial(data,oid):
    if len(data)==8:
        # the compact format. 8 bytes of serial.
        return data
    elif len(data)==12:
        # The old format. 4 bytes of magic number, then 8 bytes of serial.
        if data[:4]!=CMAGIC:
            raise DirectoryStorageError('Bad magic number in oid pointer %r' % (oid2str(oid),))
        return data[4:12]
    else:
        raise DirectoryStorageError('Bad oid pointer file for oid %r' % (oid2str(oid),))


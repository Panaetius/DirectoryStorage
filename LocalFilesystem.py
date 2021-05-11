# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

from __future__ import nested_scopes

import os, sys, time, re, threading, Queue, errno, tempfile
from BaseFilesystem import BaseFilesystem, BaseFilesystemTransaction, FileDoesNotExist

from utils import z64, oid2str, DirectoryStorageError, RecoveryError
from utils import logger
from formats import formats

class LocalFilesystem(BaseFilesystem):
    # Implementation of higher level transactional database operations,
    # using the filesystem operations defined in the base class, in
    # a manner suitable for a traditional filesystem.

    def __init__(self,dirname):
        self.dirname = os.path.abspath(dirname)
        self.snapshot_code = 'startup'
        self._have_flushed = 1
        self._shutdown_flusher = 0
        BaseFilesystem.__init__(self)
        # a dictionary containing the location to look up files, if the
        # current version is not in the normal location.
        self.relocations = {}
        self.relocations_lock = threading.Lock()
        # For production, IO overhead is reduced by dealing
        # with journal flushing in big batches. The parametes control
        # how big the batches are.
        self.flush_interval = self.config.getint('journal','flush_interval')
        self.flush_file_threshold = self.config.getint('journal','flush_file_threshold')
        self.flush_transaction_threshold = self.config.getint('journal','flush_transaction_threshold')
        self.quick_shutdown = self.config.getint('filesystem','quick_shutdown')
        self.format = self.config.get('structure','format')
        if not formats.has_key(self.format):
            raise DirectoryStorageError('Unknown format %r' % (format,))
        self._init_munger(self.format)
        self._unflushed_timestamp = 0
        self._unflushed_total = 0
        self._broken_flusher = 0
        self._flush_lock = threading.RLock()
        self._unflushed = []
        self._async_work_queue = Queue.Queue()
        self._backlog_tokens = Queue.Queue()
        for i in range(self.config.getint('journal','backlog')):
            self._backlog_tokens.put(None)

    def engage(self,synchronous=0):
        try:
            self._lock()
        except:
            raise DirectoryStorageError('Storage is locked by another process')
        try:
            self.half_relock()
        except:
            if synchronous:
                raise DirectoryStorageError('Storage is locked in snapshot mode')
            stay_in_snapshot = 1
        else:
            self._unlink_snapshot_file()
            stay_in_snapshot = 0
        self._flusher = threading.Thread(target=self._flusher)
        self._flusher.setDaemon(1)
        self._flusher.start()
        self._snapshot_lock = threading.Lock()
        self._snapshot_ack = threading.Lock()
        # Read the journal, and asynchronously move the files asynchronously into the main
        # database. These go into the B directory first because we start
        # up in snapshot mode.
        self._recovery()
        if stay_in_snapshot:
            # Some other process is assuming snapshot mode. Dont break its
            # assumptions. How do we get out of this state?
            logger.info('Engaging in snapshot mode')
        else:
            # The B directory may contain some files, whether from an old snapshot
            # or the preceeding journal flush. Move them into the A directory
            # asynchronously
            self.leave_snapshot('startup')
        # If the synchronous flag is set then ensure that the
        # B directory is flushed before this method returns.
        if synchronous:
            # nasty nasty polling implementation. doing this
            # quickly makes for pleasant unit tests
            i = 0.01
            while self.snapshot_code:
                time.sleep(i)
                i = min(5,i*1.1)

    def _init_munger(self,format):
        self.filename_munge = formats[format]

    def name(self):
        return '%s filesystem at %r' % (self.format,self.dirname)

    def _add_to_flush_queue(self,transaction):
        # no need to worry about concurrency here, because it is
        # only called from transaction commit which is already non reentent
        now = time.time()
        if not self._unflushed:
            self._unflushed_timestamp = now
        size = len(transaction.names)
        self._unflushed.append(transaction.done_name)
        self._unflushed_total += size
        age = now - self._unflushed_timestamp
        # We allow multiple transactions to pile up in the journal directory before
        # flushing them into the main directory as a batch. This batching improves
        # performance; files that were overwritten in subsequent transactions can be
        # unlinked rather than moved, and our filesystem can most likely combine
        # many of these writes. We only allow a fixed number of batches to remain
        # unflushed, controlled by _backlog_tokens queue in MultiFlush.
        # This batching can lead to unsmooth peformance under *very* heavy
        # write pressure. Does anyone see this except in a benchmark? We could
        # solve this by reducing the size of the thresholds below as the backlog increases.
        # I wont bother doing this unless someone actually needs it.
        if age > self.flush_interval:
            reason = 'age limit reached'
        elif self._unflushed_total >= self.flush_file_threshold:
            reason = 'files limit reached'
        elif len(self._unflushed) >= self.flush_transaction_threshold:
            reason = 'transactions limit reached'
        else:
            return
        # We have very old unflushed transactions, very many
        # unflushed objects, or very many unflushed transactions.
        # Start flushing now.
        self._flush_all(reason)
        self._unflushed_timestamp = now

    def _pre_transaction(self):
        pass

    def _flush_all(self,reason):
        # Move all unflushed transactions into the work queue so
        # that the other thread can deal with them
        self._flush_lock.acquire()
        try:
            if self._unflushed:
                MultiFlush(self._unflushed,self,reason).go()
                self._unflushed = []
                self._unflushed_total = 0
        finally:
            self._flush_lock.release()

    def _flusher(self):
        # function which runs in a seperate thread, to asychronously flush
        # files from completed transactions into the main database directory
        if 0:
            return
        while 1:
            work = self._async_work_queue.get()
            if self._shutdown_flusher or work is None:
                return
            else:
                try:
                    work()
                except:
                    # Argh! a problem flushing the journal. We must
                    # *never* flush any more transactions out of the
                    # journal until this problem has been addressed
                    self._broken_flusher = 1
                    self._log_broken_flusher()
                    raise

    def _log_broken_flusher(self):
        try:
            # Severity level of PANIC is perhas a little extreme.
            # Nothing is corrupted, and this situation _should_
            # be automatically recovered once the underlying problem
            # is solved. However from this point on the journal will
            # not be flushed, and eventually we will stop allowing
            # write requests
            logger.critical(
                'Error when flushing already committed transaction',
                error=sys.exc_info())
        except:
            pass

    def read_database_file(self,name):
        self.relocations_lock.acquire()
        try:
            return self._do_read_database_file(name)
        finally:
            self.relocations_lock.release()

    def _do_read_database_file(self,name):
        relocated_dir = self.relocations.get(name)
        if relocated_dir is None:
            # No relocations....
            name = self.filename_munge(name)
            if self.snapshot_code and self._have_flushed:
                # We are in snapshot mode, so we need to look in
                # directory B, followed by A
                try:
                    return self.read_file(os.path.join('B',name))
                except FileDoesNotExist:
                    return self.read_file(os.path.join('A',name))
            else:
                # If we are not in snapshot mode, or have not flushed the journal
                # since entering snapshot mode, then directory A is the only
                # place we need to look
                return self.read_file(os.path.join('A',name))
        else:
            # a relocation!!!
            try:
                return self.read_file(os.path.join(relocated_dir,name))
            except FileDoesNotExist:
                # The file given in the relocation does not exist?
                # something got deleted from the journal directory, or we corrupted
                # our relocations database. This is bad.
                logger.critical('File missing from journal')
                raise FileMissingFromJournalError()

    def _move_to_database_directory(self,sourcedir,dirmap):
        # Move many files from the journal transaction directory to
        # an appropriate database directory
        if self.snapshot_code:
            # First, record the fact that we have flushed so that _do_read_database_file
            # in snapshot mode has to do a little more work.
            self._have_flushed = 1
            dir = 'B'
        else:
            dir = 'A'
        for sname in self.listdir(sourcedir):
            if self._shutdown_flusher:
                return
            name = self.filename_munge(sname)
            directory = os.path.split(name)[0]
            dest = os.path.join(dir,name)
            self._check_dir(dest,dirmap)
            # On ext2 filesystem this is unsafe. The destination
            # directory has just been created and we are about to
            # rename comitted files into it. If the system goes down soon
            # then this directory creation may get lost. If this applies
            # to you; get a better filesystem.
            self.relocations_lock.acquire()
            try:
                relto = self.relocations.get(sname)
                if relto==sourcedir or relto==None:
                    # If this record name was previously relocated to the file
                    # we have just moved then we need to move it because it is still
                    # current. If it was not in the relocations map then we must be performing
                    # recovery, and therefore we need to move it into the database directory.
                    self.overwrite(os.path.join(sourcedir,sname),dest)
                    # Remove the relocation.
                    if relto is not None:
                        del self.relocations[sname]
                else:
                    # This record is relocated somewhere else. That means
                    # this record was overwritten while still in the journal.
                    # We could treat it the same as the first branch, but it
                    # is more efficient to remove it.
                    self.unlink(os.path.join(sourcedir,sname))
            finally:
               self.relocations_lock.release()

    def _check_dir(self,file,dirs):
        # make sure that it is possible to write the file by creating any
        # intermediate directories. dirs is a dictionary set in which we
        # record every directory modified; they may all need to be synced
        parent = os.path.split(file)[0]
        if not parent:
            return
        if not dirs.has_key(parent):
            dirs[parent] = 1
            if not self.exists(parent):
                self._check_dir(parent,dirs)
                self.mkdir(parent)

    def close(self):
        quick = self.quick_shutdown
        if not quick:
            try:
                self.enter_snapshot('shutdown')
            except DirectoryStorageError:
                logger.warning('shutdown without snapshot')
                quick = 1
        if quick:
            # Signal that we want to stop as soon as possible
            self._shutdown_flusher = 1
        # Put a notice to stop working into the work queue
        self._async_work_queue.put(None)
        # Wait for the thread to terminate.
        self._flusher.join()

    _transaction_directory_re = re.compile('^working_[A-F0-9]{16}_((?:temp)|(?:done))')
    def _recovery(self):
        # This is called at startup to flush any changes remaining
        # in the journal directory. It is always called before the
        # journal flushing thread is started. At startup we are
        # always in snapshot mode, therefore these files are being
        # flushed into the B directory
        jc = [x for x in self.listdir('journal')]
        jc.sort() # alphabetic sort order will give us jounal replay order too

        # Should we check that the journal directory names correspond to transactions
        # that are more recent than what we believe to be the most recent flushed transaction?
        # That would protect against misguided backup/restore cycles, but is there a danger
        # of false positives?

        if self.exists('journal/replica.tar'):
            # Check some things that should never happen
            if len(jc)!=1:
                raise RecoveryError('journal not empty, and journal/replica.tar exists')
            if [x for x in self.listdir('B')]:
                raise RecoveryError('B not empty, and journal/replica.tar exists')
            self.flush_replica()
            jc = []

        # first verify that there is nothing strange in the journal directory
        strange = []
        to_flush = []
        to_delete = []
        for file in jc:
            match = self._transaction_directory_re.match(file)
            if not match:
                strange.append(file)
            elif match.group(1)=='done':
                to_flush.append(file)
            else:
                to_delete.append(file)
        if strange:
            raise RecoveryError('unexpected files in journal directory: %r' % (strange))
        # For every directory that we want to keep...
        paths = []
        for file in to_flush:
            # add every file in the directory into the relocations mapping.
            path = os.path.join('journal',file)
            paths.append(path)
            for file in self.listdir(path):
                self.relocations[file] = path
        # Asynchonously move good files into the main directory
        MultiFlush(paths,self,'recovery').go()
        # And ansynchronously delete bad ones
        if to_delete:
            t = threading.Thread(target=self._delete, args=(to_delete,))
            t.setDaemon(1)
            t.start()

    def _delete(self,to_delete):
        # for every directory that we want to delete
        for file in to_delete:
            # ... delete its contents
            path = os.path.join('journal',file)
            for file in self.listdir(path):
                self.unlink(os.path.join(path,file))
            # ... and delete the directory
            self.rmdir(path)

    def flush_replica(self):
        # This is called by _recovery and by the replication tool to synchronously
        # flush HOME/journal/replica.tar, a file created by the replication process.
        tf = tempfile.TemporaryFile()
        # First use tar to list the files and test that the file is OK.
        cmd = 'tar -B -C %s -t -f %s/journal/replica.tar' % (self.dirname,self.dirname)
        f = os.popen(cmd)
        while 1:
            line = f.readline()
            if not line:
                break
            tf.write(line)
        if f.close():
            raise DirectoryStorageError('Error in replica.tar')
        # Next extract the files
        cmd = 'tar -B -C %s -x -f %s/journal/replica.tar' % (self.dirname,self.dirname)
        if os.system(cmd):
            raise DirectoryStorageError('Error unpacking replica.tar')
        # Sync all the files, using the list we prepared earlier
        # (Waaah - tar should have a mode to do this for me)
        tf.seek(0,0)
        c = 0
        while 1:
            line = tf.readline().strip()
            if not line:
                break
            self.sync_directory(line)
            self.sync_directory(os.path.split(line)[0])
            c += 1
        # Everything is safe, so we can remove the tar original.
        # Keeping the file around until the next replica is handy for
        # debugging, plus its mtime is useful if you need to know
        # when you last replicated.
        self.rename('journal/replica.tar','misc/replica.previous')
        self.sync_directory('journal')
        logger.log(self.ENGINE_NOISE, 'Flushed %d files from replica' % (c,))

    def enter_snapshot(self,code):
        # Enter snapshot mode. On return the main 'A' directory is a snapshot of the
        # database, which this class will not modify until 'leave_snapshot' is called. Note
        # that it is not possible to re-enter snapshot mode for some time after leaving
        # a previous snapshot, since database writes have to be recombined into the
        # main 'A' database directory.
        assert code, 'code must be non-zero'
        # The snapshot lock is used to ensure that only one thread enters snapshot mode,
        # and to ensure that there are no concurrent writes during entry to snapshot mode.
        self._snapshot_lock.acquire()
        try:
            if self.snapshot_code:
                raise DirectoryStorageError('Can not enter snapshot mode: snapshot already in use by %r' % self.snapshot_code)
            # We want the journal to be as empty as possible when entering snapshot mode.
            # Move all transactions into the flush queue
            self._flush_all('snapshot')
            self._unflushed_timestamp = time.time()
            # Wait for the flusher thread to flush everything, and signal that the
            # flusher thread has entered snapshot mode
            self._snapshot_ack.acquire()
            self._async_work_queue.put(lambda: self._enter_snapshot(code))
            self._snapshot_ack.acquire()
            self._snapshot_ack.release()
            logger.log(self.ENGINE_NOISE, 'Entered snapshot mode %r' % (code,))
            # Create the file which indicates that A is a snapshot
            self.write_file('misc/snapshot',code)
        finally:
            self._snapshot_lock.release()

    def _enter_snapshot(self,code):
        # Called in the flusher thread, therefore there is no problem with concurrent writes.
        self.snapshot_code = code
        self._snapshot_ack.release()

    def leave_snapshot(self,code):
        if code!=self.snapshot_code:
            raise DirectoryStorageError('bad code %r!=%r' % (code,self.snapshot_code))
        # Retract our promise to keep A as a snapshot
        self._unlink_snapshot_file()
        # We can not leave snapshot mode immediately because there are
        # files in the B directory that need to be moved into A. Do this asynchronously
        # in the flusher thread.
        self.snapshot_code = 'recombining/'+self.snapshot_code
        if code!='startup':
            logger.log(self.ENGINE_NOISE, 'Preparing to leave snapshot mode %r' % (code,))
        # We are not out of snapshot mode yet. We still need to recombine any
        # writes flushed during this period. Note that we do no call flush_all here. If we have only
        # been in snapshot mode briefly there is a good chance that all writes will still be in the
        # journal, none will have been flushed to 'B', and the recombining process will involve
        # zero work.
        self._async_work_queue.put(self._recombine)

    def _unlink_snapshot_file(self):
        try:
            self.unlink('misc/snapshot')
        except FileDoesNotExist:
            pass

    def _recombine(self,counter=None):
        # Called from the flusher thread.
        if counter is None:
            counter = self.flush_file_threshold
        try:
            self._recombine_dir('.',counter)
        except QuickExitFromRecombine:
            # Push outselves onto the back of the work queue
            # Escalate the number of files checked each time, to ensure
            # that recombination finishes in bounded time
            self._async_work_queue.put(lambda: self._recombine(1+int(counter*1.4)))
        else:
            # B directory is currently empty, and we must ensure that it
            # stays that way before we start flushing the journal into A
            self.sync_directory('B')
            logger.log(self.ENGINE_NOISE, 'Left snapshot mode %r'
                       % (self.snapshot_code,))
            self.snapshot_code = None
            self._have_flushed = 0

    def _recombine_dir(self,directory,counter):
        for file in self.listdir(os.path.join('B',directory)):
            path = os.path.join(directory,file)
            b = os.path.join('B',path)
            if self.isdir(b):
                a = os.path.join('A',path)
                # Create any directories needed for this file.
                self._check_dir(os.path.join(a,'xxxxx'),{})
                counter = self._recombine_dir(path,counter)
                self.rmdir(b)
            else:
                self.overwrite(b,os.path.join('A',path))
                if counter:
                    counter -= 1
                    if not counter:
                        # Having dealt with our quota of files it is time to
                        # free up the flusher thread and check the journal.
                        raise QuickExitFromRecombine()

    def first_half_write_file(self,filename,content):
        # Write those bytes to the specified file, and return an object
        # that can be passed to second_half_write_file or
        # abort_half_write_file
        # Many writes will overlap
        raise NotImplementedError('first_half_write_file')

    def second_half_write_file(self,cookie):
        # Commit the previously written bytes to stable storage
        raise NotImplementedError('first_half_write_file')

    def abort_half_write_file(self,f):
        # Dont waste any more time writing this file to stable storage.
        # This may or may not unlink the file
        raise NotImplementedError('first_half_write_file')


class LocalFilesystemTransaction(BaseFilesystemTransaction):

    # BaseStorage maintains a commit lock that ensures that only one instance
    # will be in use at one time.

    def __init__(self,filesystem,tid):
        self.filesystem = filesystem
        self.tid = tid
        # the name of our transaction directory
        dirname = oid2str(tid)
        self.temp_name = os.path.join('journal','working_%s_temp' % (dirname,))
        self.done_name = os.path.join('journal','working_%s_done' % (dirname,))
        self.curr_name = self.temp_name
        # mapping from name to pair of sort key and half written file in the transaction directory
        self.names = {}
        # create a transaction directory inside the journal directory
        self.filesystem.mkdir(self.temp_name)

    def write(self,name,data):
        file = self.filesystem.first_half_write_file(os.path.join(self.temp_name,name),data)
        old = self.names.get(name,None)
        if old is not None:
            self.filesystem.abort_half_write_file(old[1])
        pair = len(self.names),file
        self.names[name] = pair

    def vote(self):
        # Should we rename the directory to working_xxxx_vote in here? That would mean we
        # would have to handle the extra name in the journal recovery. If the system halts
        # after .vote() but before the .finish(), then there might be good data in this
        # directory. Ok, its a slim chance that anyone would bother, but the directory
        # is not *certainly* useless. It might be useful to simply ignore the working_xxxx_vote
        # name in recovery, to allow manual inspection.
        pass

    def finish(self):
        # First, sync all our files: body and inode
        # Do this in write order
        unwritten = self.names.values()
        unwritten.sort()
        for f in unwritten:
            self.filesystem.second_half_write_file(f[1])
        # sync the transaction directory. at this point
        # only the journal directory remains unsynced
        self.filesystem.sync_directory(self.temp_name)
        # rename the directory so that recovery knows the
        # transaction has been committed
        self.filesystem.rename(self.temp_name,self.done_name)
        # sync the journal directory, the directory which contains
        # the transaction directory. everything is now safe
        self.filesystem.sync_directory('journal')
        # register the transaction directory as containing the current
        # copy of all of these files, in case they have to be read
        # before the flush is complete.
        changes = {}
        for name in self.names.keys():
            changes[name] = self.done_name
        # Dont update relocations while another thread is entering snapshot mode.
        # They need the journal to be empty, to ensure that all files are properly flushed
        # into the snapshot
        lock1 = self.filesystem._snapshot_lock
        # Dont update relocations while another thread is not be expecting it
        lock2 = self.filesystem.relocations_lock
        lock1.acquire()
        lock2.acquire()
        try:
            self.filesystem.relocations.update(changes)
        finally:
            lock2.release()
            lock1.release()
        # flush the journal directory asynchronously. This will be
        # done in directory order
        self.filesystem._add_to_flush_queue(self)

    def abort(self):
        # close any files we might still have open
        for f in self.names.values():
            self.filesystem.abort_half_write_file(f[1])
        # Should we do the rest of this work in the other thread?
        # delete the files
        for name in self.names.keys():
            try:
                self.filesystem.unlink(os.path.join(self.temp_name,name))
            except EnvironmentError:
                pass
        # delete the transaction directory
        try:
            self.filesystem.rmdir(self.temp_name)
        except EnvironmentError:
            pass


class MultiFlush:
    def __init__(self,directories,filesystem,reason):
        self.directories = directories
        self.filesystem = filesystem
        self.reason = reason

    def go(self):
        # First we get backlog token to ensure that there are not too many
        # of us in the work queue. This might stall whoever wanted to use us
        self.filesystem._backlog_tokens.get()
        # Put ourself in the work queue
        self.filesystem._async_work_queue.put(self.flush)

    def flush(self):
        # Called from the flusher thread to flush multiple transactions
        logger.log(self.filesystem.ENGINE_NOISE, 'Flushing %d transactions (%s)'
                   % (len(self.directories),self.reason))
        dirmap = {}
        # Move many files from the journal directory to the database directory
        for directory in self.directories:
            self.filesystem._move_to_database_directory(directory,dirmap)
            if self.filesystem._shutdown_flusher:
                return
        # we are done with these transaction directories, so can safely delete them
        for directory in self.directories:
            try:
                self.filesystem.rmdir(directory)
            except EnvironmentError:
                pass
        # Now we have completed flushing all of those, put an extra token
        # in the backlog queue. This possibly enabled another transaction to finish,
        # if there was a large backlog of finished but unflushed ones
        self.filesystem._backlog_tokens.put(0)



class QuickExitFromRecombine(Exception):
    pass


class FileMissingFromJournalError(Exception):
    pass

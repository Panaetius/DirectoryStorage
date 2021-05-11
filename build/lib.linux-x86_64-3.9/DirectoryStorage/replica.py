#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

from __future__ import nested_scopes
import sys, getopt, os, time, string, stat, binascii, md5, binascii, tempfile, stat, traceback

try:
    import ZODB
except ImportError:
    print >> sys.stderr, 'Failure to import ZODB is often caused by an incorrect PYTHONPATH environment variable'
    raise

from ZODB.POSException import POSError
from utils import oid2str, ConfigParser, tid2date, format_filesize, DirectoryStorageError
from formats import formats
from snapshot import snapshot
import Full
from PosixFilesystem import PosixFilesystem
from pipeline import pipeline

# path in which this script lives. We assume whatsnew.py is in the same place
if __name__=='__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
mypath = os.path.split(os.path.abspath(mypath))[0]



def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:vqe:", [])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    if len(args) != 2:
        sys.exit(usage())
    remoteargs, localargs = args
    if remoteargs=='+':
        # We are running on the master
        r = replica_master(localargs)
    else:
        # We are running on the replica
        r = replica_slave(remoteargs, localargs)
    for o, a in opts:
        if o == '-v':
            r.verbose += 1  
        elif o == '-q':
            r.verbose -= 1
        elif o == '-d':
            r.ripath = a
        elif o == '-e':
            a = a.split()
            if a:
                r.ssh = a
        else:
            sys.exit(usage())
    try:
        r.main()
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())

        
class replica_slave:
    def __init__(self, remoteargs, localargs):
        self.verbose = 0
        self.localargs = localargs
        i = remoteargs.find(':')
        if i<0:
            self.rdir = remoteargs
            self.local = 1
        else:
            self.rhost = remoteargs[:i]
            self.rdir = remoteargs[i+1:]
            j = self.rhost.find('@')
            if j<0:
                self.user = None
            else:
                self.user = self.rhost[:j]
                self.rhost = self.rhost[j+1:]
            self.local = 0
            self.ssh = [ '/usr/bin/ssh' ]
            if self.user:
                self.ssh.extend( [ '-l', self.user ] )
            self.rpython = sys.executable
            self.ripath = mypath

    def main(self):
        if self.local:
            cmd = [ sys.executable, os.path.join(mypath,'replica.py') ] 
        else:
            cmd = self.ssh[:]
            cmd.extend ( [ self.rhost, self.rpython, os.path.join(self.ripath,"replica.py") ] )
        for i in range(0,self.verbose):
            cmd.append('-v')
        for i in range(0,self.verbose,-1):
            cmd.append('-q')
        cmd.append('+')
        cmd.append(self.rdir)
        if self.verbose>=2:
            print >> sys.stderr, repr(cmd)
        self.replica_main(cmd, self.localargs)
        if self.verbose>=0:
            print >> sys.stderr, 'Replica complete'

    def replica_main(self, cmd, path):
        # resync the replica
        self.path = path
        # Start the flush
        self.prep_replica()
        # Get a copy of the differences between the remote and
        # local storages in a local tar file.
        self.rpc(cmd)
        if self.verbose>=1:
            print >> sys.stderr, 'Flushing increment....'
        self.fs.flush_replica()
        self.fs.close()

    def prep_replica(self):
        if self.verbose>=1:
            print >> sys.stderr, 'Preparing local storage....'
        # Bring up the filesystem
        self.fs = PosixFilesystem(self.path)
        # Start the filesystem flushing itself
        # We get an exception here if we cannot lock the local
        # storage, for example if there is a live storage open
        # or another concurrent replica.py
        try:        
            self.fs.engage(synchronous=1)
        except POSError:
            raise ReplicaError('Can not engage replica storage')
        # synchronous mode engage means we know that the journal and
        # B directories are fully flushed at this point.
        self.config = self.fs.config
        self.filename_munge = self.fs.filename_munge
        self.config.read(self.path+'/config/settings')
        if self.config.get('storage','classname')!='Full':
            raise ReplicaError('This is not a Full storage')

    def rpc(self,cmd):
        # Get a copy of the differences between the remote and local storages
        # in a local tar file.
        if self.verbose>=1:
            print >> sys.stderr, 'Fetching increment....'
        request = self.get_request()
        p = pipeline()
        tarname = os.path.join(self.path,'misc','.replica.incoming')
        tarfd = os.open(tarname,os.O_RDWR|os.O_CREAT,0640)
        p.set_output(tarfd)

        def ssh():
            os.execv(cmd[0],cmd)

        w = p.pipe_input()
        p.run( ssh )
       
        os.write(w,request)
        os.close(w)
        
        p.close()

        if not p.all_ok():
            raise ReplicaError('Error from remote replica')

        if self.verbose>=1:
            size = os.fstat(tarfd)[stat.ST_SIZE]
            print >> sys.stderr, 'Increment size %s, syncing....' % (format_filesize(size),)

        # sync stuff to make it all durable
        os.fsync(tarfd)
        os.close(tarfd)
        self.fs.sync_directory('misc')
        
        # Atomically commit this replica increment
        self.fs.rename('misc/.replica.incoming','journal/replica.tar')
        self.fs.sync_directory('journal')
            
    def get_request(self):
        # Compute a string to send into stdin on our remote process to
        # tell it where to start replicating from
        #
        # Our identity. This ensures we are not replicating from the wrong
        # storage - a common mistake if you have more than one
        identity = open(os.path.join(self.path,'config','identity')).readline().strip()
        # The last transaction that is held here
        current_tid = open(os.path.join(self.path,'A',self.filename_munge('x.serial'))).read()
        # A checksum of that transaction. This ensures that our source and destination
        # are identical up to that point
        if current_tid=='\x00'*8:
            tdata = ''
        else:
            tdata = open(os.path.join(self.path,'A',self.filename_munge(Full._tid_filename(current_tid)))).read()
        return 'replica request 0\n%s\n%s\n%s\n' % ( identity, binascii.b2a_hex(current_tid), binascii.b2a_hex(md5.md5(tdata).digest()) )


                                                                                                                                                
class replica_master:
    def __init__(self, directory):
        self.path = directory
        self.verbose = 0

    def main(self):
        if not os.path.exists(os.path.join(self.path,'A')):
            raise ReplicaError('%s is not a DirectoryStorage directory.' % self.path)
        if sys.stdin.readline().strip()!='replica request 0':
            sys.exit(usage())
        identity1 = sys.stdin.readline().strip()
        identity2 = open(os.path.join(self.path,'config','identity')).readline().strip()
        if identity1!=identity2:
            raise ReplicaError('different identity')
        old_tid = binascii.a2b_hex(sys.stdin.readline().strip())
        thash1 = binascii.a2b_hex(sys.stdin.readline().strip())
        self.config = ConfigParser()
        self.config.read(self.path+'/config/settings')
        format = self.config.get('structure','format')
        self.filename_munge = formats[format]
        packed_tid = open(os.path.join(self.path,'A',self.filename_munge('x.packed'))).read()
        if packed_tid>old_tid:
            raise ReplicaError('storage has been packed since the last replica ( %s > %s )' %(tid2date(packed_tid),tid2date(old_tid)) )
        try:
            if old_tid=='\x00'*8:
                tdata = ''
            else:
                tdata = open(os.path.join(self.path,'A',self.filename_munge(Full._tid_filename(old_tid)))).read()
        except EnvironmentError:
            raise ReplicaError('reference transaction does not exist')
        if md5.md5(tdata).digest()!=thash1:
            raise ReplicaError('reference transaction differs')
       
        s = snapshot(self.path, '-', verbose=self.verbose)
        s.acquire()
        try:
            # The following section uses two child processes. The first is
            # the whatsnew script, which writes a list of files to a pipe.
            # The second is cpio, which reads file names from the pipe and
            # writes a tar file to a temporary file.
            p = pipeline()
            tf = tempfile.TemporaryFile()

            def whatsnew():
                os.chdir(self.path)
                cmd = [ sys.executable,  mypath+'/whatsnew.py' ]
                for i in range(0,self.verbose):
                    cmd.append('-v')
                for i in range(0,self.verbose,-1):
                    cmd.append('-q')
                cmd.append(oid2str(old_tid))
                if self.verbose>=2:
                    print >> sys.stderr, repr(cmd)
                os.execv(cmd[0],cmd)

            def cpio():
                os.chdir(self.path)
                os.execlp('cpio',     'cpio', '--quiet', '-o', '-H', 'ustar' )

            p.set_output(tf.fileno())
            p.run(whatsnew,cpio)
            p.close()
            e1,e2 = p.codes
            if e1:
                raise ReplicaError('error %d in whatsnew' % e1)
            if e2:
                raise ReplicaError('error %d in cpio' % e2)
                
        finally:
            # release the snapshot quickly
            s.release()
        # Trickle the tar file back to the client
        if self.verbose>=1:
            print >> sys.stderr, 'Transferring increment....'
        tf.seek(0,0)
        while 1:
            c = tf.read(1024*64)
            if not c:
                break
            sys.stdout.write(c)

        
class ReplicaError(DirectoryStorageError):
    pass
     

def usage():
    return """Usage: %s [options] [user@]MasterMachine:MasterDirectory LocalReplicaDirectory

A DirectoryStorage replication tool using ssh.

options:

 -d directory

    Specifies the installation directory on the remote machine. replica.py
    is assumed to be in this directory. By default it assumes the same
    directory as on the local machine

 -v -q
    more or less verbose
    
 -h

    Show this help

 -e COMMAND

    Use specified command instead of /usr/bin/ssh for communication to the master.
    COMMAND may also include parameters, for example -e "/usr/bin/ssh -p 1234"

""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

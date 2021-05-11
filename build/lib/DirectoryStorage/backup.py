#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

from __future__ import nested_scopes

import sys, getopt, os, time, string, stat, binascii, traceback
from DirectoryStorage.utils import oid2str, ConfigParser, format_filesize, tid2date, DirectoryStorageError
from DirectoryStorage.formats import formats
from DirectoryStorage.pipeline import pipeline
from DirectoryStorage.snapshot import snapshot


# path in which this script lives. We assume whatsnew.py is in the same place
if __name__=='__main__':
    mypath = sys.argv[0]
else:
    mypath = __file__
mypath = os.path.split(os.path.abspath(mypath))[0]



def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ['storage='])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    storage = None
    for o, a in opts:
        if o == '--storage':
            storage = a
    try:
        s = snapshot(storage)
        s.acquire()
        try:
            backup_main(s.path,s.snapshot_time,args)
        finally:
            s.release()
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())
    
def backup_main(path,timestamp,argv):
    if len(argv)<1:
        sys.exit(usage())
    b = backup(path,timestamp)
    try:
        b.main(argv)
    except:
        b.abort()
        raise
    else:
        b.commit()

class backup:
    def __init__(self,path,timestamp):
        self.prefix = 'backup'
        self.timestamp = timestamp
        now = time.time()
        if self.timestamp>now:
            sys.exit('ERROR: timestamp in the future')
        if self.timestamp<now-60*60*12:
            sys.exit('ERROR: timestamp too far in the past')
        self.path = path
        if not os.path.exists(os.path.join(self.path,'A')):
            sys.exit('ERROR: %s is not a DirectoryStorage directory.' % self.path)
        if not os.path.exists(os.path.join(self.path,'backups')):
            sys.exit('ERROR: %s/backups does not exist.' % self.path)
        self.config = ConfigParser()
        self.config.read(self.path+'/config/settings')
        if self.config.get('storage','classname')!='Full':
            sys.exit('ERROR: this is not a Full storage')
        format = self.config.get('structure','format')
        if not formats.has_key(format):
            sys.exit('ERROR: Unknown format %r' % (format,))
        self.filename_munge = formats[format]
        self.current_tid = open(os.path.join(self.path,'A',self.filename_munge('x.serial'))).read()
        self._prepare_index()
        self.renames = []

    def main(self,args):
        while args:
            arg, args = args[0], args[1:]
            if arg=='prefix':
                self.prefix,args = args[0], args[1:]
            elif arg=='full':
                self._full_backup()
            elif arg=='inc':
                # Create an incremental backup
                recent,args = args[0], args[1:]
                try:
                    self._incremental_backup(parse_time(recent))
                except NoPreviousBackups:
                    print >> sys.stderr, traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip()
            else:
                sys.exit('ERROR: unknown command %r' % arg)

    def _prepare_index(self):
        try:
            self.logf = open(os.path.join(self.path, 'backups/tindex'), 'r+')
        except:
            self.logf = open(os.path.join(self.path, 'backups/tindex'), 'w+')
            self.logf.write(tindex_header())
            self.logf.flush()
            self.seq = 1
        else:
            # find the largest sequence number, Its probably on the last line,
            # but its easier to run through them all
            lines = self.logf.readlines()
            seq = 0
            for line in lines:
                line = string.split(line,'#',1)[0]
                line = string.split(line)
                if line:
                    seq = max(seq,int(line[0]))
            self.seq = seq+1
        print >> sys.stderr, 'This is backup sequence number %d' % (self.seq,)

    def _full_backup(self):
        # Perform a full backup using tar
        filename = '%s/backups/%s-%s.tgz' % (self.path,self.prefix,self.seq)
        print >> sys.stderr, 'Creating full backup %r' % (filename,)
        tmpfilename = '%s/backups/.tmp-%s-%s.tgz' % (self.path,self.prefix,self.seq)

        p = pipeline()

        def find():
            os.chdir(self.path)
            for name in os.listdir('.'): 
                sys.stdout.write(name+'\n')       # just the directory - not its content
            for name in os.listdir('config'): 
                sys.stdout.write('config/'+name+'\n')
            sys.stdout.flush() 
            os.execlp('find',      'find', 'A', '-type', 'f', '-not', '-name', '*-deleted' )

        def cpio():
            os.chdir(self.path)
            os.execlp('cpio',      'cpio', '--quiet', '-o', '-H', 'ustar' )

        def gzip():
            os.execlp('gzip',      'gzip' )

        fd = os.open(tmpfilename,os.O_WRONLY|os.O_CREAT,0640)
        p.set_output(fd)
        p.run( find, cpio, gzip )
        p.close()
        if not p.all_ok():
            sys.exit(1)
        else:
            os.fsync(fd)
            os.close(fd)
            self.renames.append((tmpfilename,filename))
            print >> sys.stderr, '    full backup complete, %s' % (filesize(tmpfilename))

    def _incremental_backup(self,recent):
        # 'recent' is a timestamp. We should ignore all backups made after 'recent'
        # when chosing which one to use as our reference for this incremental
        # backup
        oldseq,timestamp,tid = self.find_rev(recent)
        # Now we have the sequence number, timestamp, and transaction id of our incremental
        # backup reference.
        if tid==self.current_tid and not self.renames:
            # The most recent transaction in that backup is the same as the current most
            # recent transaction. That means there is nothing to be backed up.
            print >> sys.stderr, "Ignoring empty incremental backup"
            return
        # Use whatsnew.py to determine the names of all files modified in
        # transactions since that one, use cpio to copy them into an archive, and gzip it.
        filename = '%s/backups/%s-%s-to-%s.tgz' % (self.path,self.prefix,oldseq,self.seq)
        tmpfilename = '%s/backups/.tmp-%s-%s-to-%s.tgz' % (self.path,self.prefix,oldseq,self.seq)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(timestamp))
        print >> sys.stderr, 'Creating incremental backup %r\n    since backup %d at %s\n    which included transaction %s %s' % (filename,oldseq,timestamp,oid2str(tid),tid2date(tid))
        
        p = pipeline()

        def whatsnew():
            cmd = [ sys.executable,  mypath+'/whatsnew.py' ]
            cmd.append(oid2str(tid))
            os.execv(cmd[0],cmd)

        def cpio():
            os.chdir(self.path)
            os.execlp('cpio',      'cpio', '--quiet', '-o', '-H', 'ustar' )

        def gzip():
            os.execlp('gzip',      'gzip' )
        
        fd = os.open(tmpfilename,os.O_WRONLY|os.O_CREAT,0640)
        p.set_output(fd)
        p.run( whatsnew, cpio, gzip )
        p.close()
        if not p.all_ok():
            sys.exit(1)
        else:
            os.fsync(fd)
            os.close(fd)
            self.renames.append((tmpfilename,filename))
            print >> sys.stderr, '    incremental backup complete, %s' % (filesize(tmpfilename))


    def find_rev(self,goal):
        prev_seq = None
        for line in open(os.path.join(self.path, 'backups/tindex'), 'r').readlines():
            line = string.split(line,'#',1)[0]
            line = string.split(line)
            if line:
                seq = int(line[0])
                timestamp = int(line[1])
                tid = line[2]
                if timestamp>goal:
                    break
                else:
                    prev_seq,prev_timestamp,prev_tid = seq,timestamp,tid
        # a backup made after our target timestamp; use the previous one
        if prev_seq is None:
            raise NoPreviousBackups('ERROR: No previous backup found. incremental backup impossible')
        else:
            return prev_seq,prev_timestamp,binascii.a2b_hex(prev_tid)

    def commit(self):
        if self.renames:
            self.logf.seek(0,2)
            self.logf.write('%-5d %-12d %s\n' % (self.seq,self.timestamp,oid2str(self.current_tid)))
            # TODO fsync it
            while self.renames:
                tmpfilename,filename = self.renames.pop()
                os.rename(tmpfilename,filename)
        else:
            print >> sys.stderr, 'No backups needed, not writing an entry into the index file'

    def abort(self):
        while self.renames:
            tmpfilename,filename = self.renames.pop()
            os.unlink(tmpfilename)

def parse_time(t):
    # Parse the command line parameter which contains the incremental backup
    # timestamp reference.
    try:
        # Try parse it as an integer
        return int(t)
    except ValueError:
        # Pass it to the 'date' command, which does a nice job of parsing times.
        # This allows nice things like "36 hours ago". Currently this uses a GNU
        # extension to dump the date as seconds since the epoch.
        f = os.popen('date --date "'+t+'" +%s')
        c = f.read()
        if f.close():
            raise ValueError(t)
        return int(c)
        

class NoPreviousBackups(Exception):
    pass

def filesize(filename):
    size = os.stat(filename)[stat.ST_SIZE]
    return format_filesize(size)


def tindex_header():
    return """
# This file is an index of incremental backups. Each row contains the following columns:
# sequence number
#     timestamp of the backup
#                  last transaction id included in the backup

"""

def usage():
    return """Usage: %s [options] [commands]

A DirectoryStorage backup tool using gnu tar. This tool can take
full and incremental backups of a DirectoryStorage database
snapshot. 

Options are:

    --storage DIRECTORY
    
        The full path to the storage. May only be omitted if run
        under the snapshot.py tool.

Commands are:

    prefix p

        Set the prefix for backup filenames

    full

        Create a full backup in (directory)/backups/(prefix)-nnn.tgz

    inc TIMESTAMP

        Create an incremental backup containing all the changes since
        the first backup earlier than that timestamp. timestamp is
        in any format understood by the 'date' command (such as
        "36 hours ago"), or can be specified in seconds since the
        epoch. The backup tar file is written to
        (directory)/backups/(prefix)-mmm-to-nnn.tgz
""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

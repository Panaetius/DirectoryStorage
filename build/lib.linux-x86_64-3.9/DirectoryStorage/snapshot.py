#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, sys, time, urllib, string, base64, socket, getopt, traceback

try:
    import ZODB
except ImportError:
    print >> sys.stderr, 'Failure to import ZODB is often caused by an incorrect PYTHONPATH environment variable'
    raise

from zc.lockfile import LockFile
from ZODB.POSException import POSError
import xmlrpclib
from utils import DirectoryStorageError
from Filesystem import Filesystem

class xmlrpclib_auth_Transport(xmlrpclib.Transport):
    def __init__(self,username,password):
        self.auth = 'Basic '+string.replace(base64.encodestring('%s:%s' % (username,password)),'\012','')

    def send_user_agent(self,connection):
        connection.putheader('Authorization',self.auth)
        connection.putheader("User-Agent", 'DirectoryStorage snapshot.py tool')

class URLopener(urllib.FancyURLopener):
    def __init__(self,name,password):
        urllib.FancyURLopener.__init__(self,{})
        self.name,self.password = name,password
    def prompt_user_passwd(self, host, realm):
        return self.name,self.password


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vqt:s:", ['storage=','snapshot-conf='])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    verbose = 0
    end_time = time.time()
    sleep_time = 10
    path = None
    config = '-'
    for o, a in opts:
        if o == '--storage':
            path = a
        elif o == '--snapshot-conf':
            config = a
        elif o == '-v':
            verbose += 1
        elif o == '-q':
            verbose -= 1
        elif o=='-t':
            end_time += int(a)
        elif o=='-s':
            sleep_time = int(a)
    if path is None:
        # The pre-1.1.10 command line format
        if len(args) < 3:
            sys.exit(usage())
        path,config,command = args[0], args[1], args[2:]
    else:
        command = args[:]
    try:
        while 1:
            try:
                s = snapshot(path,config,verbose)
                s.acquire()
            except DirectoryStorageError:
                if time.time()>end_time:
                    raise
                else:
                    if verbose>=0:
                        msg = traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip()
                        print >> sys.stderr, 'Retrying snapshot.... (%s)' % msg
                    time.sleep(sleep_time)
            else:
                break
        if command:
            exit_code = os.spawnvp(os.P_WAIT,command[0],command)
        else:
            exit_code = 0
        s.release()
        sys.exit(exit_code)
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())

class snapshot:
    def __init__(self,path,config='-',verbose=0):
        self.verbose = verbose
        self.path = path
        self.config = config

    def acquire(self):
        ver = os.environ.get('SNAPSHOT_VERSION')
        if ver is None:
            if self.path is None:
                raise DirectoryStorageError('Must be run with --storage parameter, or under snapshot.py tool')
        elif ver!='2':
            raise DirectoryStorageError('ERROR: wrong version of snapshot.py tool')
        else:
            env_path = os.environ['SNAPSHOT_DIRECTORY']
            if self.path is not None and self.path != env_path:
                raise DirectoryStorageError('directories different')
            self.snapshot_time = int(os.environ['SNAPSHOT_TIMESTAMP'])
            self.path = env_path
            self.closer = None
            self.f = None
            return

        if not os.path.exists(os.path.join(self.path,'A')):
            raise DirectoryStorageError('%r is not a DirectoryStorage directory.' % self.path)
        if self.config == '-':
            self.config = os.path.join(self.path,'config','snapshot.conf')

        self.snapshot_time = time.time()
        closer = process_config_file(self.config,self.verbose,self.path)
        # At this point we are in one of four states:
        # 1. We have forced the storage into snapshot mode, closer is not None
        #    and we can therefore certainly lock the directory
        # 2. closer is None because Zope is not running. If the storage shutdown
        #    cleanly then we can lock the directory anyway.
        # 3. closer is None because the storage crashed. Locking will succeed, but
        #    we will not be in a snapshot
        # 4. closer is None because we were unable to communicate with
        #    the running storage to force it into snapshot mode. Bad
        #    DirectoryStorageToolkit configuration? Bad URLs? Overzealous
        #    firewall? Locking will fail.
        # 5. Something else is using the directory
        #    Locking will fail.
        #
        # The procedure for establishing this is documented in doc/snapshot
        f = LockFile(os.path.join(self.path, 'misc/sublock'))
        if self.verbose>=0:
            print >> sys.stderr, 'Locked snapshot mode'
        if not os.path.exists(os.path.join(self.path, 'misc/snapshot')):
            if closer is not None:
                closer()
            raise DirectoryStorageError('directory is not a snapshot')
        self.closer = closer
        self.f = f
        os.environ['SNAPSHOT_VERSION'] = '2'
        os.environ['SNAPSHOT_DIRECTORY'] = self.path
        os.environ['SNAPSHOT_TIMESTAMP'] = str(int(self.snapshot_time))

    def release(self):
        if self.f is not None:
            self.f.close()
            del os.environ['SNAPSHOT_VERSION']
            del os.environ['SNAPSHOT_DIRECTORY']
            del os.environ['SNAPSHOT_TIMESTAMP']
            if self.verbose>=0:
                print >> sys.stderr, 'Unlocked snapshot mode'
        if self.closer is not None:
            self.closer()
            if self.verbose>=0:
                print >> sys.stderr, 'Left snapshot mode'


def process_config_file(config,verbose,path):
    number = 0
    code = socket.gethostname()+'-'+str(os.getpid())
    use_last_resort_direct = 1
    try:
        lines = open(config).readlines()
    except EnvironmentError:
        if verbose>=0:
            print >> sys.stderr, 'failed to open %s'  % (config,)
        lines = ['direct']
    for line in lines + ['last_resort_direct']:
        number += 1
        line = line.strip().split()
        if not line:
            pass
        elif len(line)==3 and line[0]=='http':
            url,access = line[1], line[2]
            access_filename = os.path.join(os.path.split(config)[0],access)
            access_data = open(access_filename).readline()
            name,password = [ s.strip() for s in access_data.split(':')[:2] ]
            toolkit = xmlrpclib.Server(url,transport=xmlrpclib_auth_Transport(name,password))
            try:
                toolkit.enterSnapshot(code)
            except xmlrpclib.Fault:
                pass
            except socket.error:
                pass
            else:
                def closer(toolkit=toolkit,code=code):
                    toolkit.leaveSnapshot(code)
                if verbose>=0:
                    print >> sys.stderr, 'Entered snapshot using %s'  % (url,)
                return closer
        elif line[0] == 'nodirect':
            use_last_resort_direct = 0
        elif line[0] in ('direct','last_resort_direct'):
            if line[0] == 'direct' or (line[0] == 'last_resort_direct' and use_last_resort_direct):
                use_last_resort_direct = 0
                try:
                    fs = Filesystem(path)
                    fs.engage(synchronous=1)
                    fs.quick_shutdown = 0
                    fs.close()
                except DirectoryStorageError:
                    pass
                else:
                    if verbose>=0:
                        print >> sys.stderr, 'Entered snapshot using direct access'
                    return None
        else:
            if verbose>=0:
                print >> sys.stderr, 'malformed line %d in %s' % (number,config)
            # formerly a hard error - not since 1.1.12
    return None


# On Monday 20 January 2003 10:09 am, Toby Dickenson wrote:
# > On Monday 20 January 2003 2:55 am, Adrian van den Dries wrote:
# > > I have one feature request, though: a utility to put the storage into
# > > snapshot mode without calling a Zope URL. If I'm using ZEO with the
# > > extension methods patch, is it just a matter of instantiating a
# > > ClientStorage and calling enter_snapshot()?
# >
# > I originally planned to support this in snapshot.py. It would be useful,
# > but there is a subtle security risk.... A characterstic of the current ZEO
# > protocol is that both ends must trust each other. If your Zope is
# > compromised then it is easy for an attacker to run arbitrary code inside
# > the ZEO server, and from there it can run arbitrary code on any
# > ClientStorage that connects to it. snapshot.py is often run from
# > privelidged admin accounts, so connecting using ClientStorage would be
# > dangerous.



def usage():
    return """Usage: %s [options] COMMAND

A DirectoryStorage snapshot tool.

  COMMAND
    A command to execute once the storage has been locked
    into snapshot mode.

options:

  --storage DIRECTORY
    Indicate the DirectoryStorage home directory. The --storage
    switch may only be omitted if you are the old (pre 1.1.10)
    command line syntax.

  -v -q
    More or less verbose.

  -t SECONDS
    If something else has locked snapshot mode, keep retrying
    for the specified number of seconds. (default: no retries)

  -s SECONDS
    Sleep for the specified number of seconds between retries.
    (default: 10 seconds)

  --snapshot-conf FILE
    Override the default path to the snapshot.conf configuration
    file. The default (or if you specify the value "-" here)
    is DIRECTORY/config/snapshot.conf

Lines in the snapshot configuration file can be in the following form.
It will try each one in in turn until one succeeds.

  http URL ACCESS
    Force the storage into snapshot mode using http.
    The URL must refer to a DirectoryStorageToolkit instance.
    ACCESS is a file which contains username and password on
    one line seperated by a colon. Note that this format is
    compatible with zope's 'access' file, although passwords
    must be unencrypted.

  direct
    Open the files direct. Only works if nothing else is
    using the storage

Once in snapshot mode this tool will lock the directory. It only
executes the command if the locking is successful. The command is
executed with the following environment variables defined:

  SNAPSHOT_VERSION
    The value "2". This may change in future versions of the snapshot
    program where commands may be executed in a different environment.

  SNAPSHOT_DIRECTORY
    The directory from the snapshot.py command line

  SNAPSHOT_TIMESTAMP
    A timestamp shortly before entering snapshot mode
""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

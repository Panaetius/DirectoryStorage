#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, sys, time, struct, md5, getopt, traceback

from ZODB import TimeStamp
from DirectoryStorage.utils import ZODB_referencesf

from DirectoryStorage.Filesystem import Filesystem
from DirectoryStorage.formats import formats
from DirectoryStorage.utils import timestamp2tid, oid2str, CMAGIC, OMAGIC, TMAGIC, FileDoesNotExist, z64, z128
from DirectoryStorage.utils import ConfigParser, class_name_from_pickle, DirectoryStorageError
from DirectoryStorage.snapshot import snapshot

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vq", ['storage='])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    storage = None
    verbose = 0
    for o, a in opts:
        if o == '--storage':
            storage = a
        elif o == '-v':
            verbose += 1
        elif o == '-q':
            verbose -= 1
    if len(args) != 0:
        sys.exit(usage())
    try:
        s = snapshot(storage,verbose=verbose)
        s.acquire()
        try:
            checkds(s.path,verbose)
        finally:
            s.release()
    except DirectoryStorageError:
        sys.exit(traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip())
    if verbose>=0:
        print >> sys.stderr, 'done'

    
def checkds(directory,verbose):
    if not os.path.exists(directory):
        sys.exit('ERROR: directory does not exist')
    c = CoreChecker(directory,verbose)
    c.check()



class BaseChecker:
    def __init__(self,directory,verbose=0):
        self.directory = directory
        self.verbose = verbose
        self.output = sys.stderr
        self.filesystem = Filesystem(directory)

    def panic(self,msg):
        sys.exit('ERROR: '+msg)


class CoreChecker(BaseChecker):

    def check(self):
        self.sanity()
        self.storage()

    def sanity(self):
        if self.verbose>=0:
            print >> self.output, "Basic sanity-checking of the directory structure..."
        for file in ['A','config/settings','config/identity']:
            if not self.filesystem.exists(file):
                self.panic('directory fails basic sanity test; %r is missing' % (file,))
        for file in ['misc','B','journal']:
            if not self.filesystem.exists(file):
                self.panic('directory %r is missing (solution; create an empty one)' % (file,))

    def storage(self):
        if self.verbose>=0:
            print >> self.output, "Determine how we check the storage, then do it..."
        config = ConfigParser()
        config.read(self.directory+'/config/settings')
        classname = config.get('storage','classname')
        if classname == 'Full':
            FullChecker(self.directory,self.verbose).check(config)
        else:
            sys.exit('checker for class %r not yet implemented' % (classname,))

class FullChecker(BaseChecker):
    def check(self,config):
        self.config = config
        self.format = self.config.get('structure','format')
        if not formats.has_key(self.format):
            self.panic('Unknown format %r' % (self.format,))
        self.filename_munge = formats[self.format]
        self.check_roots()
        self.traverse_all()

    def read_database_file(self,file):
        return self.filesystem.read_file('A/'+self.filename_munge(file))

    def check_roots(self):
        if self.verbose>=0:
            print >> self.output, 'checking root files...'
        self.old_oid = self.read_database_file('x.oid')
        if len(self.old_oid)!=8:
            self.panic('bad length of old oid file')
        self.old_serial = self.read_database_file('x.serial')
        if len(self.old_serial)!=8:
            self.panic('bad length of old serial file')
        self._last_pack = self.read_database_file('x.packed')
        if len(self._last_pack)!=8:
            self.panic('bad length of last pack file')
        # Allow 60 seconds for clock shear
        today = time.time()+60
        t = timestamp2tid(today)
        # Here we check whether the 'largest ever serial number' is in the future.
        # Later we check whether all actual serial numbers are smaller than it.
        if self.old_serial>t:
            self.panic('timestamps in the future')
        if self._last_pack>t:
            self.panic('last pack time in the future')

    def traverse_all(self):
        root = '\0'*8
        self.stats = {}
        if self.verbose>=0:
            print >> self.output, 'unmarking...'
        self.mc = self.filesystem.mark_context('A/')
        if self.verbose>=0:
            print >> self.output, 'checking all transaction files...'
        self.check_history(self.old_serial)
        if self.verbose>=0:
            print >> self.output, 'checking all data files...'
        self.is_problem = 0
        self.traverse(root)
        for k,v in self.stats.items():
            if self.verbose>=0:
                print >> self.output, '%10d %s' % (v,k)
        if self.is_problem:
            self.panic('problems found in data files')

    def problem(self,n,info):
        self.counter(n)
        if self.verbose>=-1:
            print >> self.output, '%s: %s' % (n,info)
        self.is_problem = 1

    def counter(self,n,info=None,size=1):
        stats = self.stats
        stats[n] = stats.get(n,0L)+size

    def check_history(self,tid):
        fs = self.filesystem
        while 1:
            name = _tid_filename(tid)
            name = fs.filename_munge(name)
            name = os.path.join('A',name)
            strtid = oid2str(tid)
            try:
                data = self.filesystem.read_file(name)
            except FileDoesNotExist:
                if tid>=self._last_pack:
                    self.panic('broken history')
                else:
                    # earlier transactions have been lost to packing
                    return
            self.counter('transactions')
            self.counter('bytes in transaction files',size=len(data))
            self.counter('total bytes',size=len(data))
            self.mc.mark(name)
            if self._check_transaction_file(tid,data,name):
                break
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
            # dont check the oidblock in detail yet
            nexttid = data[24:32]
            if nexttid>tid:
                self.problem('transaction file with backwards next-transaction pointer',name)
                break
            tid = nexttid
            if tid==z64:
                # this was the first revision ever
                break

    def _check_transaction_file(self,tid,data,name):
        strtid = oid2str(tid)
        if TMAGIC!=data[:4]:
            self.problem('transaction files with a bad magic number',name)
            return 1
        apptid = data[8:16]
        if tid!=apptid:
            self.problem('transaction files with an inconsistent transaction id',name)
            return 1
        l = struct.unpack('!I',data[4:8])[0]
        if l!=len(data):
            self.problem('transaction files with an inconsistent length',name)
            return 1
        md5sum = data[32:48]
        vdata = data[48:]
        if md5sum==z128 :
            self.counter('transaction files with no checksum')
        else:
            if md5.md5(vdata).digest()!=md5sum:
                self.problem('transaction files with a bad md5 checksum',name)
                return 1

    # This is fairly similar to packing
    def traverse(self,oid):
        todo = {oid:[]}
        max_length = 0
        total_length = 0L
        n = 0
        while todo:
            # The current ZODB oids assignment policy means that chosing the
            # largest oid in the todo list leads to keeping a small todo list
            oid = max(todo.keys())
            route = todo[oid]
            new = self.traverse_impl(oid,route) or {}
            todo.update(new)
            del todo[oid]
            max_length = max(max_length,len(todo))
            total_length += len(todo)
            n += 1
        if total_length:
            if self.verbose>=0:
                print >> self.output, '%10d maximum referencee graph depth' % (max_length,)
                print >> self.output, '%10.1f average reference graph depth' % (total_length/float(n),)

    def print_route(self,route,this_oid,this_tid=None,this_class_name=None):
        print >> sys.stderr, "Object reference chain (root oid first):"
        for class_name,oid,tid in route:
            print >> sys.stderr, "  %s\n    %s\n    tid %s" % (oid2str(oid), class_name, oid2str(tid))
        print >> sys.stderr, "  %s" % oid2str(this_oid)
        if this_class_name:
            print >> sys.stderr, "    %s" % this_class_name
        if this_tid:
            print >> sys.stderr, "    tid %s" % oid2str(tid)

    def traverse_impl(self,oid,route):
        fs = self.filesystem
        stroid = oid2str(oid)
        name = 'o'+stroid+'.c'
        name = fs.filename_munge(name)
        name = os.path.join('A',name)
        if self.mc.is_marked(name):
            # This object has already been marked,
            # so there is nothing more to do.
            return
        self.mc.mark(name)
        self.counter('objects')
        try:
            data = fs.read_file(name)
        except FileDoesNotExist:
            if self.verbose>=1:
                self.print_route(route,oid)
            # This used to be reported as 'objects with missing c-file'
            self.problem('dangling reference',name)
            return
        except EnvironmentError:
            self.problem('objects with unreadable c-file',name)
            return
        if len(data)==8:
            current = data
        elif len(data)==12:
            self.counter('objects using the old 12 byte c-file format')
            if data[:4]!=CMAGIC:
                self.problem('objects with a corrupt c-file',name)
                return
            else:
                current = data[4:]
        else:
            self.problem('objects with a c-file of unusual length',name)
            return
        # Next, check the files containing recent revisions of this object
        # to determine the set of referenced objects
        tid = current
        allrefoids = {}
        first = 1
        while 1:
            # Load this revision
            strtid = oid2str(tid)
            name = 'o'+stroid+'.'+strtid
            name = fs.filename_munge(name)
            name = os.path.join('A',name)
            try:
                data = fs.read_file(name)
            except FileDoesNotExist:
                if tid>=self._last_pack or first:
                    # Missing file. This indicates database corruption.
                    self.problem('objects with missing data file',name)
                    return
                else:
                    # Revision does not exist. It must have been removed by packing
                    break
            self.counter('bytes in object files',size=len(data))
            self.counter('total bytes',size=len(data))
            self.counter('revisions of objects')
            if self.check_object_file(oid,tid,data,name):
                break
            pickle = data[72:]
            if len(pickle)==0:
                # an object whose creation has been undone.
                # This revision references nothing
                pass
            else:
                class_name = class_name_from_pickle(pickle)
                # Record the referenced objects in the to-do list
                try:
                    refoids = []
                    ZODB_referencesf(pickle,refoids)
                    for refoid in refoids:
                        allrefoids.setdefault(refoid, route + [ (class_name, oid, tid), ] )
                except (IOError,ValueError,EOFError),e:
                    if first:
                        self.problem('bad pickle in current data', name)
                        return
                    else:
                        self.problem('bad pickle in historic data', name)
                    raise
            # Check the corresponding transaction file
            tname = _tid_filename(tid)
            tname = fs.filename_munge(tname)
            tname = os.path.join('A',tname)
            if fs.exists(tname):
                if tid<'\x03F%\xa0\x00\x00\x00\x00':
                    # A bug in a pre-alpha version of DirectoryStorage can cause this. If the
                    # transaction timestamp is earlier than this bug fix, then allow this minor
                    # problem to pass silently. This choice of timestamp ensures that the
                    # problem will not no unnoticed in released versions.
                    pass
                else:
                    if not self.mc.is_marked(tname):
                        self.problem('data files whose transaction file is not linked into history', name)
            else:
                # in version 1.0 this was reported as a strong warning. In practice little depended on
                # this invariant, and they are occasionally generated harmlessly when a pack
                # is interrupted. In version 1.1 a number of features have been added that make this
                # normal, although none of these features are on by default.
                self.counter('data files with no corresponding transaction file')
            nexttid = data[56:64]
            if nexttid>tid:
                self.problem('object file with backwards revision pointer', name)
                break
            tid = nexttid
            if tid==z64:
               break
            first = 0
        return allrefoids

    def check_object_file(self,oid,serial,data,name):
        # Given the body of an object file, check as much as we can. Its
        # redundant oid, its redundant serial number (if known), its
        # redundant length, and md5 checksum of the whole file
        if OMAGIC!=data[:4]:
            self.problem('object data files with a bad magic number',name)
            return 1
        l = struct.unpack('!I',data[4:8])[0]
        if l!=len(data):
            self.problem('object data files with an inconsistent length',name)
            return 1
        appoid = data[8:16]
        if oid!=appoid:
            self.problem('object data files with an inconsistent oid',name)
            return 1
        otherserial = data[16:24]
        # otherserial is the serial number of the revision that this data was
        # copied from during an undo.
        if otherserial>=serial:
            self.problem('object data files with a backwards undo pointer',name)
            return 1
        if data[24:40]!=z128:
            self.problem('object data files with non-zero bits in the reserved area',name)
            return 1
        md5sum = data[40:56]
        serials_plus_pickle = data[56:]
        if md5sum==z128:
            self.counter('object data files with no md5 checksum')
        else:
            if md5.md5(serials_plus_pickle).digest()!=md5sum:
                self.problem('object data files with bad md5 checksum',name)
                return 1
        appserial = serials_plus_pickle[8:16]
        if serial is not None and serial!=appserial:
            self.problem('object data files with inconsistent serial number',name)
            return 1


def _tid_filename(tid):
    return 't%02X%02X%02X.%02X%02X%02X%02X%02X' % struct.unpack('!8B', tid)


def usage():
    return """Usage: %s [options]

A DirectoryStorage checking tool. This tool needs to lock the storage
into snapshot mode - it can do so directly, or it can be run under
the snapshot.py.

options:

 --storage DIRECTORY
    Indicate the DirectoryStorage home directory. May be omitted
    if this tool is being run under the snapshot.py tool.
 
 -v -q
    More or less verbose.
    

""" % os.path.basename(sys.argv[0])


if __name__=='__main__':
    main()

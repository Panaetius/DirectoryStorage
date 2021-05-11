import sys, os, errno, binascii, struct
from utils import oid2str, ConfigParser, DirectoryStorageError, tid2date
from formats import formats
from Full import _tid_filename as Full_tid_filename

class FullSimpleIterator:
    # Most ZODB storages have an iterator() method, but the semantics of that
    # are too poorly defined. Instances of this object also have an iterator, and 
    # can therefore stand in for a storage in some circumstances. This method might
    # get promoted to the main storage object if anyone would find that useful,
    # and can better definine exactly what this interface should do.
    #
    # Want to use this iterator method to build a copyTransactionFrom script?
    # check out ds2fs.py
    #
    #
    # The iterator method of this class uses transaction metadata to list
    # object revisions. It does not check md5 checksums, or verify the database
    # integrity. It will get all objects if everything is as expected, and if
    # checkds shows no warnings. It is NOT fault tolerant, and might eat your
    # data. Caution is advised.
    #
    # Further, it is definitely not compatible with some non-default values
    # for the storage/keep_policy configuration file option, which intentionally
    # drop some transaction metadata to save disk space.
    #
    def __init__(self,dspath,verbose):
        self.used = 0
        self.dspath = dspath
        self.verbose = verbose
        self.config = ConfigParser()
        self.config.read(dspath+'/config/settings')
        if self.config.get('storage','classname')!='Full':
            sys.exit('ERROR: this is not a Full storage')
        format = self.config.get('structure','format')
        if not formats.has_key(format):
            sys.exit('ERROR: Unknown format %r' % (format,))
        self.filename_munge = formats[format]
        
    def close(self):
        pass
        
    def iterator(self):
        if self.used:
            raise ValueError('can only be called once')
        self.used = 1
        # FIXME: will this list of transaction ids get too long to conveniently fit in memory?
        self.find_all_transaction_ids()
        self.all_transaction_ids.reverse()
        self.object_rev_count = 0
        return self
        
    def find_all_transaction_ids(self):
        self.all_transaction_ids = r = []
        if self.verbose>=0:
            print >> sys.stderr, 'Finding all DirectoryStorage Transaction Ids.....'
        old_tid = tid = self.read('x.serial')
        while 1:
            strtid = oid2str(tid)
            try:
                data = self.read(Full_tid_filename(tid))
            except IOError,e:
                if e.errno == errno.ENOENT:
                    break
                else:
                    raise
            r.append(tid)
            old_tid = tid
            tid = data[24:32]
        if self.verbose>=0:
            print >> sys.stderr, 'Found %d Transactions.....' % len(self.all_transaction_ids)
        if self.verbose>=1:
            print >> sys.stderr, 'Earliest transaction file is %s dated %s.....' % (binascii.b2a_hex(old_tid),tid2date(old_tid))
            print >> sys.stderr, 'Transaction before that one is %s dated %s.....' % (binascii.b2a_hex(tid),tid2date(tid))
    
    def read(self,database_filename):
        return open(os.path.join(self.dspath,'A',self.filename_munge(database_filename))).read()
    
    def __getitem__(self,i):
        # Waaaah - using an iterator would be so much nicer, but we want to be compatible with python 2.1.
        return TransactionRecord(self,self.all_transaction_ids[i])

class TransactionRecord:
    def __init__(self,it,tid):
        self._it = it
        # These two values are needed by copyTransactionsFrom
        self.tid = tid
        self.status = ' ' 
        #
        strtid = oid2str(tid)
        data = self._it.read(Full_tid_filename(tid))
        lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
        # These three value are needed when copyTransactionsFrom uses
        # this object as the transaction parameter to tpc_begin
        self.user = data[60:60+lenu]
        self.description = data[60+lenu:60+lenu+lend]
        self._extension = data[60+lenu+lend:60+lenu+lend+lene]
        # used for traversal later
        self.oidblock = data[60+lenu+lend+lene:60+lenu+lend+lene+leno]
        assert 0==(len(self.oidblock)%8)
        
    def __getitem__(self,i):
        strtid = oid2str(self.tid)
        while self.oidblock:
            oid,self.oidblock = self.oidblock[:8],self.oidblock[8:]
            stroid = oid2str(oid)
            try:
                odata = self._it.read('o'+stroid+'.'+strtid)
            except IOError,e:
                if e.errno == errno.ENOENT:
                    pass
                else:
                    raise
            else:
                prevtid = odata[56:64]
                if len(odata)==72:
                    # George Bailey object
                    data = None
                else:
                    data = odata[72:]
                return ObjectRevisionRecord(oid,self.tid,data)
        raise IndexError(i)

class ObjectRevisionRecord:
    version = ''
    data_txn = None
    def __init__(self,oid,tid,data):
        self.oid = oid
        self.serial = tid # for older zodb version
        self.tid = tid    # for newer
        self.data = data


# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import struct, time, binascii, cPickle

from ZODB import POSException
from ZODB.TimeStamp import TimeStamp

try:
    import zLOG
    have_zlog = 1
except ImportError:
    have_zlog = 0

if have_zlog:
    # Zope 2.x
    import zLOG
    loglevel_INFO = zLOG.INFO

    class Logger:
        """Stripped down implementation of a logger; barely fulfills
        our needs to cope with Python 2.1/Zope 2.6 backward
        compatability."""

        def __init__(self, name):
            self.name = name

        def log(self, level, msg):
            zLOG.LOG(self.name, level, msg)

        def info(self, msg):
            self.log(zLOG.INFO, msg)

        def error(self, msg):
            self.log(zLOG.ERROR, msg)

        def critical(self, msg):
            self.log(zLOG.PANIC, msg)

    logger = Logger('DirectoryStorage')
else:
    # Zope 3.1
    import logging
    logger = logging.getLogger('DirectoryStorage')
    loglevel_INFO = logging.INFO

try:
    # Zope 2.8 and Zope 3.1
    from ZODB.loglevels import BLATHER as loglevel_BLATHER
except ImportError:
    # Zope <2.8
    from zLOG import BLATHER as loglevel_BLATHER

try:
    # This works on Zope 2.6 and 2.7
    from ZODB.referencesf import referencesf as ZODB_referencesf
except ImportError:
    # This on Zope 2.8
    from ZODB.serialize import referencesf as ZODB_referencesf

from ConfigParser import ConfigParser as BaseConfigParser
from ConfigParser import Error as ConfigParserError

z16='\0'*2
z64='\0'*8
z128='\0'*16

# the first four bytes of object files, used by Full and Minimal
OMAGIC = '\xbd\xb8*q'

# the first four bytes of transaction files, used only by Full
TMAGIC = 'G@\x07v'

# what used to be the first four bytes of oid current revision
# pointer file, used only by Full. today we care more about file
# size
CMAGIC = '\013\376\350\354'

def oid2str(oid):
    assert len(oid)==8
    return binascii.b2a_hex(oid).upper()

class DirectoryStorageError(POSException.StorageError):
    pass

class DirectoryStorageVersionError(POSException.Unsupported):
    pass

class RecoveryError(DirectoryStorageError):
    pass
    
class FileDoesNotExist(Exception):
    # Exception raised when reading a file that does not exist
    pass


def timestamp2tid(t):
    return repr(TimeStamp(*(time.gmtime(t)[:5] + (t % 60,))))

def tid2timestamp(tid):
    return TimeStamp(tid).timeTime()

def tid2date(tid):
    return time.strftime('%Y-%m-%d %H:%M:%S GMT',time.gmtime(TimeStamp(tid).timeTime()))


def format_filesize(size):
    if size<2048:
        return '%d bytes' % (size,)
    size /= 1024
    if size<2048:
        return '%d k' % (size,)
    size /= 1024
    return '%d M' % (size,)


# The name of this exception class is likely to change....
class POSGeorgeBaileyKeyError(POSException.POSKeyError):
    """Access to an object whose creation has been undone
    """
    pass


from ZODB.POSException import DanglingReferenceError

# TODO disabled this assertion because it fails on Python 2.4; it and
# the function below need to be revised for Python 2.4 compatability
#assert cPickle.dumps((('a','b'),None),1)=='((U\x01aU\x01btNt.'
def class_name_from_pickle(d):
    # Not a full unpickler - just check for the most common form
    # of a pickled Persistent class instance
    if d[:3]=='((U':
        l = ord(d[3])
        c = d[4:4+l]
        d = d[4+l:]
        if d[:3]=='q\001U':
            l = ord(d[3])
            c = c+'.'+d[4:4+l]
            d = d[4+l:]
            if d[0]=='q':
                return c


# Our options are case sensitive
class ConfigParser(BaseConfigParser):
    def optionxform(self,s):
        return str(s)


def storage_pack_days(storage,days):
    # Normally storages are packed via the DB class. DB.pack() has a nice
    # interface but storage.pack() does not. This method provides a nice
    # wrapper, similar to the DB class. It is convenient if you want to
    # pack a storage without creating a DB
    t = time.time()-(days*86400)
    storage.pack(t,ZODB_referencesf)


#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, sys, time, string, base64, traceback, struct
from ZODB.TimeStamp import TimeStamp

from utils import ZODB_referencesf
from utils import OMAGIC, TMAGIC, CMAGIC, oid2str, timestamp2tid

from formats import _chunky_munge_filename as munge
from Full import _tid_filename


def main():
    if len(sys.argv)<2:
        sys.exit(usage())
    for file in sys.argv[1:]:
        dump(file)
        print

def dump(filename):
    try:
        d = open(filename,'r').read()
    except:
        print traceback.format_exception_only(sys.exc_info()[0],sys.exc_info()[1])[0].strip()
    else:
        print filename
        if len(d)==8:
            dump_c(d)
        elif len(d)==12 and d.startswith(CMAGIC):
            dump_c(d[4:])
        elif d.startswith(OMAGIC):
            dump_o(d)
        elif d.startswith(TMAGIC):
            dump_t(d)
        else:
            print 'no idea'

def dump_o(d):
    oid = d[8:16]
    stroid = oid2str(oid)
    tid = d[64:72]
    strtid = oid2str(tid)
    print '  data for oid %s rev %s' % (stroid,strtid)
    print '  proper filename %s' % (munge('o'+stroid+'.'+strtid),)
    print '  transaction'
    print '    timestamp %s' % (time.ctime(TimeStamp(tid).timeTime()),)
    print '    filename %s' % munge(_tid_filename(tid))
    prevtid = d[56:64]
    strprevtid = oid2str(prevtid)
    print '  previous rev'
    print '    rev %s' % (strprevtid,)
    print '    filename %s' % (munge('o'+stroid+'.'+strprevtid),)
    pickle = d[72:]
    print '  pickle %r' % (pickle[:70],)
    r = []
    ZODB_referencesf(pickle,r)
    if r:
        print '  references'
        for oid in r:
            stroid = oid2str(oid)
            print '    oid %s at %s' % (stroid,munge('o'+stroid+'.c'),)
            
    
def dump_c(d):
    tid = d
    strtid = oid2str(tid)
    print '  current rev %s' % (strtid,)
    print '  transaction timestamp %s' % (time.ctime(TimeStamp(tid).timeTime()),)

def dump_t(d):
    tid = d[8:16]
    strtid = oid2str(tid)
    print 'transaction %s' % (strtid,)
    print '  timestamp %s' % (time.ctime(TimeStamp(tid).timeTime()),)
    print '  proper filename %s' % munge(_tid_filename(tid))
    lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',d[48:60])
    print '  user %r' % d[60:60+lenu]
    print '  description %r' % d[60+lenu:60+lenu+lend]
    oidblock = d[60+lenu+lend+lene:60+lenu+lend+lene+leno]
    print '  objects modified in this transaction'
    while oidblock:
        oid,oidblock = oidblock[:8],oidblock[8:]
        stroid = oid2str(oid)
        print '    oid %s at %s' % (stroid,munge('o'+stroid+'.'+strtid),)
            

def usage():
    return """Usage: %s filename

Dump DirectoryStorage info from specified files
""" % os.path.basename(sys.argv[0])


if __name__=='__main__':
    main()



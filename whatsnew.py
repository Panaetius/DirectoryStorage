import sys, getopt, os, time, string, stat, binascii, struct

from utils import oid2str, ConfigParser, tid2date

from formats import formats
from Full import _tid_filename

def main():
    ver = os.environ.get('SNAPSHOT_VERSION')
    if ver!='2':
        sys.exit('ERROR: Run this tool under snapshot.py')
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vq", [])
    except getopt.GetoptError:
        # print help information and exit:
        sys.exit(usage())
    if len(args) != 1:
        sys.exit(usage())
    verbose = 0
    for o, a in opts:
        if o == '-v':
            verbose += 1
        elif o == '-q':
            verbose -= 1
    path = os.environ['SNAPSHOT_DIRECTORY']
    limit_tid = args[0]
    if len(limit_tid)!=16:
        sys.exit('ERROR: transaction_id must be in the 16-character hexadecimal form')
    limit_tid = binascii.a2b_hex(limit_tid)
    config = ConfigParser()
    config.read(path+'/config/settings')
    if config.get('storage','classname')!='Full':
        sys.exit('ERROR: this is not a Full storage')
    format = config.get('structure','format')
    if not formats.has_key(format):
        sys.exit('ERROR: Unknown format %r' % (format,))
    filename_munge = formats[format]
    # First check that the transaction id are sensible
    current_tid = open(os.path.join(path,'A',filename_munge('x.serial'))).read()
    if current_tid<limit_tid:
        sys.exit('ERROR: specified transaction id is in the future')
    packed_tid = open(os.path.join(path,'A',filename_munge('x.packed'))).read()
    #print >> sys.stderr, 'Was packed before',oid2str(packed_tid)
    if packed_tid>limit_tid:
        sys.exit('ERROR: storage has been packed since the specified transaction')
    # Think about producing some output
    if limit_tid==current_tid:
        # No transactions since the specified time; no output
        if verbose>=0:
            print >> sys.stderr, 'No transactions since', oid2str(limit_tid), tid2date(limit_tid)
        return
    if verbose>=0:
        print >> sys.stderr, 'Newest transaction is',oid2str(current_tid), tid2date(current_tid)
    # Output the standard files that are always subject to change
    print os.path.join('A',filename_munge('x.serial'))
    print os.path.join('A',filename_munge('x.oid'))
    if 0:
        # Do not include the file that contains the date of last packing.
        # That would be approriate for a script called 'whatsold' that deals
        # with removing files, but not this 'whatsnew' script that deals
        # with adding files.
        print os.path.join('A',filename_munge('x.packed'))
    # Iterate through older transactions back to the start date
    oids = {}
    transactions = 0
    files = 3
    while limit_tid<current_tid:
        transactions += 1
        strtid = oid2str(current_tid)
        transaction_filename = os.path.join('A',filename_munge(_tid_filename(current_tid)))
        files += 1
        # Output the name of the transaction file
        print transaction_filename
        data = open(os.path.join(path,transaction_filename)).read()
        lenu,lend,lene,leno,lenv = struct.unpack('!HHHIH',data[48:60])
        oidblock = data[60+lenu+lend+lene:60+lenu+lend+lene+leno]
        # Iterate through all objects modified in this transaction
        while oidblock:
            oid,oidblock = oidblock[:8],oidblock[8:]
            stroid = oid2str(oid)
            if not oids.has_key(oid):
                # Output the name of the file that contains the current revision pointed for this
                # oid, but only once per run if it is modified in multiple transactions
                oids[oid] = 1
                print os.path.join('A',filename_munge('o'+stroid+'.c'))
                files += 1
            # Output the name of the file that contains the object data written in this transaction
            print os.path.join('A',filename_munge('o'+stroid+'.'+strtid))
            files += 1
        # Go on to the previous transaction
        current_tid = data[24:32]
    if verbose>=1:
        print >> sys.stderr, '%d files in %d objects in %d transactions' % (files, len(oids), transactions)



def usage():
    return """Usage: %s transaction_id

A tool to inspect a DirectoryStorge.Full directory while in snapshot mode
to determine the list of files modified in all trasactions after the one
specified on the command line. File names are written to stdout.

transaction_id must be in the 16-character hexadecimal form

This tool is designed to be run under snapshot.py
""" % os.path.basename(sys.argv[0])

if __name__=='__main__':
    main()

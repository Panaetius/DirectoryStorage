#!/usr/bin/python2.1
#
# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, sys, md5, mimetools, binascii

from formats import formats

def usage():
    return """Usage: %s directory <Full|Minimal> <bushy|chunky>

A tool to create a new empty storage. 

  Full      - Default DirectoryStorage type.
  Minimal   - Does not support undo, incremental backups, replication,
              versions, or packing.

  bushy     - For conventional filesystem like ext3 on Linux.
  chunky    - For filesystem which is efficient with large directories, such as
              reiserfs or JFS on Linux.
""" % os.path.basename(sys.argv[0])

def main():
    argv = sys.argv
    if len(argv)!=4:
        sys.exit(usage())
    mkds(argv[1], argv[2], argv[3])
    print >> sys.stderr, 'created OK'

def mkds(directory,classname,format,sync=1,somemd5s=1):
    if format=='auto':
        raise ValueError('Format must be specified when creating')
    if not formats.has_key(format):
        raise ValueError('Unknown format %r' % (format,))
    filename_munge = formats[format]
    if classname == 'Full':
        pass
    elif classname == 'Minimal':
        pass
    else:
        raise ValueError('Unknown class %r' % (classname))
    if os.path.exists(directory):
        raise ValueError('Cant create %r, it already exists' % (directory,))
    os.mkdir(directory)
    os.mkdir(directory+'/A')
    os.mkdir(directory+'/journal')
    os.mkdir(directory+'/B')
    os.mkdir(directory+'/misc')
    os.mkdir(directory+'/config')
    open(directory+'/config/settings','w').write(_default_settings % locals())
    open(directory+'/config/identity','w').write(make_identity())
    name = directory+'/A/'+filename_munge('x.oid')
    mkdirs(name)
    open(name,'w').write('\0'*8)
    name = directory+'/A/'+filename_munge('x.serial')
    mkdirs(name)
    open(name,'w').write('\0'*8)
    name = directory+'/A/'+filename_munge('x.packed')
    mkdirs(name)
    open(name,'w').write('\0'*8)

def mkdirs(file):
    # make sure that it is possible to write the file by creating any
    # intermediate directories
    parent = os.path.split(file)[0]
    if not parent:
        return
    if not os.path.exists(parent):
        mkdirs(parent)
        os.mkdir(parent)

def make_identity():
    # choose a random string to use as a database identity
    try:
        # use the system random device if it has one
        return binascii.b2a_hex(open('/dev/urandom').read(16))
    except EnvironmentError:
        # use a hash of python's entropy gatherer
        return binascii.b2a_hex(md5.md5(mimetools.choose_boundary()).digest())



_default_settings="""
[structure]

format: %(format)s
version: 0.11

[md5policy]

# When do we use md5?

# calculate md5 on every write. This incurs a performance cost, but
# provides a very high degree of confidence in data integrity
write: %(somemd5s)d

# check md5 on every read. This incurs a performance penalty. You may
# want to change this to '0' if your ZEO server is using alot of
# processor time
read: %(somemd5s)d

# check the md5 of the old revision of an object, before we store
# a new revision. This is only really useful if are not checking
# md5 on every read
overwrite: 0

# check when scanning the undo log
undolog: %(somemd5s)d

# check when performing an undo
undo: %(somemd5s)d

# check when loading a historical revision, or when creating a list
# of historical revisions
history: %(somemd5s)d

# check when packing
pack: %(somemd5s)d

[journal]

# IO overhead is reduced by dealing with journal flushing
# in big batches. The parameters control how big the batches are.

# max number of seconds between flushes
flush_interval: 3600

# max number of unflushed files
flush_file_threshold: 2000

# max number of unflushed transactions
flush_transaction_threshold: 200

# how many batches of transactions can be in the flush queue,
# before we start blocking writes. This is necessary to prevent
# journal overload.
backlog: 3

[storage]

# What type of storage lives here
classname: %(classname)s

# How long to we spend scanning back through old transactions
# in history and undo log operations before giving up? (in seconds)
# If your storage is published using ZEO1 (and probably ZEO2) then
# all clients are blocked during this operation. Set this value to
# zero for no timeout.
history_timeout: 10

# If zero, unreachable database files are deleted during packing.
# This is appropriate if you are using a stable version of
# DirectoryStorage, do not expect any problems, and really need
# to save a small percentage of your disk space.
#
# If greater than zero, packing will not immediately delete files.
# Instead of deleting, they are renamed such that normal access
# to the DirectoryStorage will not find them. However if you find
# that packing has incorrectly deleted a file, you can recover
# it by undoing the renaming. This option is essential if you
# are using a development version of DirectoryStorage. It is 
# on by default even for stable versions because it provides
# a useful disaster recovery capability.
#
# The value specified here is the number of seconds which
# files are left in the renamed state before permanent deletion.
# A handy value is 864000 - ten days
delay_delete: 864000

# Sometimes ZODB applications holds on to object references across
# transaction boundaries; They write a new object in one transaction,
# then write a reference to it in a later transaction. This is not
# strictly allowed, but it does happen in a number of unusual boundary
# conditions. Data loss can result if this happens at the same time
# as packing with a threshold time of zero. To avoid data loss, we
# prevent setting a pack threshold time lower than this value in
# seconds
min_pack_time: 600

# Should DirectoryStorage check that all stored object references 
# actually refer to a real object? If disabled, then subsequently
# following this reference would cause a POSKeyError. If enabled,
# this problem is detected before the transaction commits.
# This option prevents unusual boundary conditions from causing
# problems that are hard to distinguish from database corruption.
# Having this enabled is particularly useful during development.
# These potential problems are somewhat theoretical, therefore
# it is reasonably safe to disable this for a significant
# performance improvement in write-heavy applications.
check_dangling_references: 1

# A choice of policy on how much information is kept about
# ancient transactions.  This transaction information is mostly used for
# implementing undo.
#   detailed
#     The same policy as for version 1.0, FileStorage, and most other
#     storages. Full details are kept about the transaction in which
#     an object revision was written.
#   undoable
#     A space saving optimisation over 'detailed'. Packing will discard
#     transaction files older than the pack threshold. There is no
#     chance that these transactions are undoable, therefore the main
#     reason for keeping the file does not apply. The one disadvantage
#     to this approach occurs with ancient objects that have not been
#     modified since before the pack threshold date. Their history
#     (in the History tab in Zope's Management Interface) will not
#     contain the URL or username for the ancient transaction in
#     which they were modified.
keep_policy: detailed

[filesystem]

# Controls whether data is synced to stable storage at transaction
# boundaries. This is necessary for durable transactions but is an
# avoidable performance cost if you do not need durability. For
# example when using copyTransactionsFrom() to copy from one storage
# to another this saves roughly one third of runtime.
sync: %(sync)d

# When asked to shutdown the storage has two options. Firstly it
# can shutdown as quickly as possible, but this means it is impossible
# to take a backup of a shutdown storage because some transactions
# have half their files in the journal, and the other half in the
# database directory.
#
# Alternatively it can shutdown a little slower, spending a couple
# of seconds flushing the journal. This is probably a good compromise
# unless you have set the journal size to be huge.
#
# Note that it is possible to set quick_shutdown to 1, then
# manually force the storage into snapshot mode just before shutdown
# if that is what you need case by case.
#
# Note that if, when asked to close, the storage has just come out
# of snapshot mode and is still busy recombining, it always performs
# a quick shutdown. Recombination can take a very long time.
quick_shutdown: 0

[windows]
mark: attributes

[posix]

# Controls how it stores one bit of information per file to hold the
# marked/unmarked status during the mark/sweep packing.
# Values are:
#   permissions
#     scrounge a bit from the file permissions; by default the
#     'set uid on execute' bit. This is the fastest, and generally
#     safe. There is little reason to change this default
#  file
#     use a seperate empty file. Use this if you dont want to
#     play with file permissions. Beware performance is terrible.
#     Almost noone will want this.

mark: permissions



# If the filesystem/sync option is set to 1, then this controls whether
# fsync is also used for directory operations. Turning this off will
# break durability in most cases. It is necessary to get DirectoryStorage
# to run on an NFS filesystem. Durability on NFS is untested. 

dirsync: 1



# Controls whether certain classes should have their history retained
# for longer than the normal pack time. Each entry specifies the
# behavior for one class.
#
# Full.Class.Name: forever
#     This class will never have any history removed while it
#     is still reachable.
#
# Full.Class.Name: extra DAYS
#     This class will have its history kept for the specified
#     number of days longer than normal.
#
# Note that keeping extra history may not mean you can undo more,
# if these objects are modified in the same transaction as objects
# that do not have extra history.


[keepclass]


"""


if __name__=='__main__':
    main()

import sys
import threading
import time
import traceback
import unittest

import DirectoryStorage.Filesystem
import DirectoryStorage.utils
from ZODB import POSException
from ZODB.tests import (BasicStorage, ConflictResolution, Corruption,
                        HistoryStorage, IteratorStorage, MTStorage,
                        PackableStorage, PersistentStorage, ReadOnlyStorage,
                        RecoveryStorage, RevisionStorage, Synchronization,
                        TransactionalUndoStorage,
                        TransactionalUndoVersionStorage, VersionStorage)

from .DirectoryStorageTestBase import *

# threading._VERBOSE = 1


class DirectoryStorageFullTests:
    def checkRememberOid(self):
        # create an object
        oid = self._storage.new_oid()
        assert oid == "\0\0\0\0\0\0\0\1", repr(oid)
        self._dostore(oid=oid)
        # create another
        oid = self._storage.new_oid()
        assert oid == "\0\0\0\0\0\0\0\2", repr(oid)
        self._dostore(oid=oid)
        # reopen the storage
        fs = self._storage.filesystem.__class__(self._storage.filesystem.dirname)
        self._storage.close()
        self._storage = self._storage.__class__(fs)
        # check it remembered the old oids, and allocated a new one
        oid = self._storage.new_oid()
        assert oid == "\0\0\0\0\0\0\0\3", repr(oid)


class _PackableStorage(PackableStorage.PackableStorage):

    if hasattr(PackableStorage.PackableStorage, "checkPackUndoLog"):
        # Only if these test are present.... they are not in Zope 2.8
        #
        # These tests incorrectly assumes that packing will remove all unreachable
        # objects. DirectoryStorage will keep them if they were written
        # sufficiently recently. They also create dangling references.
        # Overwrite these bits of its configuration to make it behave like FileStorage
        def checkPackAllRevisions(self):
            self._storage.min_pack_time = 0
            PackableStorage.PackableStorage.checkPackAllRevisions(self)

        def checkPackJustOldRevisions(self):
            self._storage.min_pack_time = 0
            self._storage.check_dangling_references = 0
            PackableStorage.PackableStorage.checkPackJustOldRevisions(self)

        def checkPackOnlyOneObject(self):
            self._storage.min_pack_time = 0
            self._storage.check_dangling_references = 0
            PackableStorage.PackableStorage.checkPackOnlyOneObject(self)

        def checkPackUndoLog(self):
            self._storage.min_pack_time = 0
            self._storage.check_dangling_references = 0
            PackableStorage.PackableStorage.checkPackUndoLog(self)

        # def checkPackUndoLogUndoable(self):
        #    self._storage.min_pack_time = 0
        #    self._storage.check_dangling_references = 0
        #    PackableStorage.PackableStorage.checkPackUndoLogUndoable(self)

    # DirectoryStorage defines an 'empty storage' as one with no root
    # object. This is different to other storages. Here we allow it
    # to pack a storage with no root object. This is the default
    # behaviour *except* during unit tests.
    def checkPackEmptyStorage(self):
        self._storage._ok_to_pack_empty_storage = 1
        PackableStorage.PackableStorage.checkPackEmptyStorage(self)


class _TransactionalUndoStorage(TransactionalUndoStorage.TransactionalUndoStorage):
    # This test incorrectly assumes that packing works without a root object.
    # inhibit it for now. Im not worried about this - other tests have the
    # same coverage
    def checkTransactionalUndoAfterPack(self):
        pass


class FullZODBTests(
    # Same suite of tests as FileStorage except where commented,
    # or inhibited below
    StorageTestBase.StorageTestBase,
    BasicStorage.BasicStorage,
    _TransactionalUndoStorage,
    RevisionStorage.RevisionStorage,
    ####VersionStorage.VersionStorage,
    ####TransactionalUndoVersionStorage.TransactionalUndoVersionStorage,
    _PackableStorage,
    Synchronization.SynchronizedStorage,
    ConflictResolution.ConflictResolvingStorage,
    ####ConflictResolution.ConflictResolvingTransUndoStorage,
    HistoryStorage.HistoryStorage,
    ####IteratorStorage.IteratorStorage,
    ####IteratorStorage.ExtendedIteratorStorage,
    PersistentStorage.PersistentStorage,
    MTStorage.MTStorage,
    ReadOnlyStorage.ReadOnlyStorage,
    DirectoryStorageFullTests,
):
    pass


class FullTests:
    pass


class FullChunkyTest(FullChunkyBase, FullZODBTests, FullTests):
    pass


class MinimalTests(BasicStorage.BasicStorage):
    pass


class MinimalBushyTest(MinimalBushyBase, MinimalTests):
    pass


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FullChunkyTest, "check"))
    suite.addTest(unittest.makeSuite(MinimalBushyTest, "check"))
    return suite


def main():
    try:
        unittest.main(defaultTest="test_suite")
    except:
        traceback.print_exc()


if __name__ == "__main__":
    if 0:
        import trace

        tracer = trace.Trace(
            ignoredirs=[
                sys.prefix,
                sys.exec_prefix,
            ],
            trace=0,
            count=1,
        )
        tracer.runfunc(main)
        r = tracer.results()
        r.write_results(show_missing=1, summary=1)
    else:
        main()

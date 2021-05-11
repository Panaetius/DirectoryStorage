import os, shutil, sys, time
from ZODB.tests import StorageTestBase

from DirectoryStorage.mkds import mkds

directory = os.path.join(os.path.split(os.path.abspath(__file__))[0],'db')

class DirectoryStorageTestBase(StorageTestBase.StorageTestBase):
    def setUp(self):
        StorageTestBase.StorageTestBase.setUp(self)
        if os.path.exists(directory):
            shutil.rmtree(directory)
        mkds(directory,self.Storage.__name__,self.Format,sync=0)
        self.open()

    def open(self):
        fs = self.Filesystem(directory)
        self._storage = self.Storage(fs,synchronous=1)
        # help me debug the unit tests
        self._storage._ok_to_pack_empty_storage = 0
        self._storage._do_packing_in_new_thread = 0
        #print >> sys.stderr, 'setUp'

    def tearDown(self):
        # If the tests exited with any uncommitted objects, they'll blow up
        # subsequent tests because the next transaction commit will try to
        # commit those object.  But they're tied to closed databases, so
        # that's broken.  Aborting the transaction now saves us the headache.
        #print >> sys.stderr, 'tearDown'
        self._storage.close()

    def _inter_pack_pause(self):
        # for TransactionalUndoStorage
        while self._storage.filesystem.snapshot_code:
            time.sleep(0.01)

    def _make_readonly(self):
        self._storage._is_read_only = 1
        self.assert_(self._storage.isReadOnly())



class FullChunkyBase(DirectoryStorageTestBase):
    from DirectoryStorage.Full import Full as Storage
    from DirectoryStorage.Filesystem import Filesystem
    Format = 'chunky'

class MinimalBushyBase(DirectoryStorageTestBase):
    from DirectoryStorage.Minimal import Minimal as Storage
    from DirectoryStorage.Filesystem import Filesystem
    Format = 'bushy'


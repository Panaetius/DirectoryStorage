# Copyright (c) 2002-2005 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1
#
from zope.app.applicationcontrol.browser.zodbcontrol import ZODBControlView

class DirectoryStorageControlView(ZODBControlView):

    def enterSnapshot(self, code=None):
        db = self.request.publication.db
        db._storage.enter_snapshot(code or 'toolkit')
        self.request.response.redirect('@@ZODBControl.html')

    def leaveSnapshot(self, code=None):
        db = self.request.publication.db
        db._storage.leave_snapshot(code or 'toolkit')
        self.request.response.redirect('@@ZODBControl.html')

    def _currentSnapshotCode(self):
        db = self.request.publication.db
        return db._storage.get_snapshot_code()
    currentSnapshotCode = property(_currentSnapshotCode)

    def isDirectoryStorage(self):
        storage = self.request.publication.db._storage
        try:
            fn = storage.is_directory_storage
        except AttributeError:
            return False
        else:
            return fn()

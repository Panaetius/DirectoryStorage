import os, sys, time

import Globals
from OFS.SimpleItem import SimpleItem
from Products.PageTemplates.PageTemplateFile import PageTemplateFile
from AccessControl import ClassSecurityInfo

MANAGE_DS = 'Manage DirectoryStorages'

class DirectoryStorageToolkit(SimpleItem):
    """DirectoryStorageToolkit - a tool for switching DirectoryStorage
    into snapshot mode
    """
    meta_type = 'DirectoryStorage Toolkit'
    id = 'DSToolkit'

    _v_is_directory_storage = None

    security = ClassSecurityInfo()

    manage_options = (
        {'label': 'DirectoryStorage', 'action': 'manage_main'},
        ) + SimpleItem.manage_options

    manage_main = PageTemplateFile('form.pt', globals())

    security.declareProtected(MANAGE_DS, 'enterSnapshot')
    def enterSnapshot(self, code=None):
        """Enter snapshot mode"""
        self._p_jar._storage.enter_snapshot(code or 'toolkit')
        if code is None:
            self.REQUEST.RESPONSE.redirect(self.absolute_url()+'/manage_main')

    security.declareProtected(MANAGE_DS, 'leaveSnapshot')
    def leaveSnapshot(self, code=None):
        """Leave snapshot mode"""
        self._p_jar._storage.leave_snapshot(code or 'toolkit')
        if code is None:
            self.REQUEST.RESPONSE.redirect(self.absolute_url()+'/manage_main')

    security.declareProtected(MANAGE_DS,'currentSnapshotCode')
    def currentSnapshotCode(self):
        """ """
        return self._p_jar._storage.get_snapshot_code()

    security.declareProtected(MANAGE_DS, 'isDirectorystorage')
    def isDirectorystorage(self):
        if self._v_is_directory_storage is None:
            storage = self._p_jar._storage
            try:
                fn = storage.is_directory_storage
            except AttributeError:
                self._v_is_directory_storage = 0
            else:
                self._v_is_directory_storage = fn()
        return self._v_is_directory_storage

Globals.InitializeClass(DirectoryStorageToolkit)

def manage_addDirectoryStorageToolkit(dispatcher, RESPONSE=None):
    """Add a site error log to a container."""
    log = DirectoryStorageToolkit()
    dispatcher._setObject(log.id, log)
    if RESPONSE is not None:
        RESPONSE.redirect(
            dispatcher.DestinationURL() +
            '/manage_main?manage_tabs_message=DirectoryStorage+Toolkit+Added.' )

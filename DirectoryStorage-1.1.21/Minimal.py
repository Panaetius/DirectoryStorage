# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, errno, time, sys, string

from ZODB import POSException

from BaseDirectoryStorage import BaseDirectoryStorage

from utils import z64, z128, OMAGIC, oid2str, DirectoryStorageError, DirectoryStorageVersionError, FileDoesNotExist


class Minimal(BaseDirectoryStorage):

    def _load_object_file(self,oid):
        stroid = oid2str(oid)
        try:
            data = self.filesystem.read_database_file('o'+stroid)
        except FileDoesNotExist:
            raise POSException.POSKeyError(oid)
        return data, None

    def _get_current_serial(self,oid):
        # could use some caching here?
        try:
            data,serial2 = self._load_object_file(oid)
        except POSException.POSKeyError:
            return None
        self._check_object_file(oid,serial2,data,self._md5_overwrite)
        serial = data[64:72]
        return serial

    def store(self, oid, serial, data, version, transaction):
        if self._is_read_only:
            raise POSException.ReadOnlyError('Can not store to a read-only DirectoryStorage')
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if version:
            raise DirectoryStorageVersionError('Versions are not supported')
        old_serial = self._get_current_serial(oid)
        if old_serial is None:
            # no previous revision of this object
            old_serial = z64
        elif old_serial!=serial:
            # The object exists in the database, but the serial number
            # given in the call is not the same as the last stored serial
            # number.
            raise POSException.ConflictError(serials=(old_serial, serial))
        assert len(self.get_current_transaction())==8
        body = self._make_file_body(oid,self.get_current_transaction(),old_serial,data)
        self._write_object_file(oid,self.get_current_transaction(),body)
        return self.get_current_transaction()

    def _write_object_file(self,oid,newserial,body):
        td = self._transaction_directory
        stroid = oid2str(oid)
        td.write('o'+stroid,body)

    def _vote_impl(self):
        pass

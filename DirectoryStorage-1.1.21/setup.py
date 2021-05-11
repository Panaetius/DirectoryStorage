# Copyright (c) 2002-2005 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1
#
# See doc/install or http://dirstorage.sourceforge.net/install.html
# for more details on the installation process

import sys
from setuptools import setup
from distutils.extension import Extension

# TODO: install DirectoryStorage-overrides.zcml into
# $HOME/etc/package-includes if the latter exists and --home is used

packages = ['DirectoryStorage',
            'DirectoryStorage.browser',
            'DirectoryStorage.tests']
package_dir = {'DirectoryStorage': '.'}
package_data = {'DirectoryStorage':         ['component.xml'],
                'DirectoryStorage.browser': ['zodbcontrol.pt',
                                             'configure.zcml']}
ext_modules = []

if sys.platform != 'win32':
    # the readdir extension is only used by PosixFilesystem
    ext_modules.append(Extension('DirectoryStorage.readdir', ['readdir.c']))

setup(
    name="DirectoryStorage",
    version="1.1.21",
    description="A ZODB Storage which uses one file per revision of an object",
    long_description="""DirectoryStorage is a Storage for ZODB, the object database used by Zope. It
uses ordinary files and directories to store revisions of ZODB objects; one
file per revision per object.

The following features differentiate DirectoryStorage from other storages:
1. A very simple file format; one file per revision per object. Your data is
   not locked away inside an unfamiliar, opaque database.
2. Designed for disaster-preparedness. Restore service after a disaster faster
   and with greater confidence.
3. Storage Replication. Efficiently and robustly keep an online backup copy of
   your storage up to date with the latest changes from the master.
4. Designed for fault tolerance. Any bugs (in DirectoryStorage, or elsewhere)
   are less likely to irretrievably destroy your data.

And more ... look at http://dirstorage.sourceforge.net
""",
    author="Toby Dickenson and contributors",
    author_email="dirstorage-users@lists.sourceforge.net",
    maintainer="Oleksandr Kozachuk",
    maintainer_email="ddeus.dirstorage@mailnull.com",
    url="http://dirstorage.sourceforge.net",
    download_url="https://sourceforge.net/projects/dirstorage/",
    license="GPL 2.1",
    platforms=['any'],
    packages = packages,
    package_dir = package_dir,
    package_data = package_data,
    ext_modules = ext_modules,
    install_requires = ['zc.lockfile>=1.0.0'],
    entry_points = {
        'console_scripts': [
            'dirstorage_backup = DirectoryStorage.backup:main',
            'dirstorage_checkds = DirectoryStorage.checkds:main',
            'dirstorage_ds2fs = DirectoryStorage.ds2fs:main',
            'dirstorage_dumpdsf = DirectoryStorage.dumpdsf:main',
            'dirstorage_fs2ds = DirectoryStorage.fs2ds:main',
            'dirstorage_mkds = DirectoryStorage.mkds:main',
            'dirstorage_replica = DirectoryStorage.replica:main',
            'dirstorage_snapshot = DirectoryStorage.snapshot:main',
            'dirstorage_whatsnew = DirectoryStorage.whatsnew:main',
        ],
    }
)

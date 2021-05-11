# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os

if os.name=='posix':
    from PosixFilesystem import PosixFilesystem as Filesystem
elif os.name=='nt':
    from WindowsFilesystem import WindowsFilesystem as Filesystem
else:
    raise NotImplementedError('Unsupported os type %r' % (os.name,))
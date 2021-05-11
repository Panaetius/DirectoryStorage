# Copyright (c) 2005 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1
#
# See doc/install or http://dirstorage.sourceforge.net/install.html
# for more details on the installation process

all:
	python setup.py build_ext --inplace

clean:
	rm -Rf build
	rm -f *.so
	rm -f `find . -name "*.pyc" -print`

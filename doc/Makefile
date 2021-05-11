# This Makefile is used to prepare the documentation
# files for CVS and the sourceforge website
#
# Only the DirectoryStorage documentation maintainer
# needs to use this Makefile.

all:
	echo "Only the DirectoryStorage documentation maintainer needs to use this Makefile"

publish: htmlfiles
	scp -r htdocs htrd@shell.sourceforge.net:/home/groups/d/di/dirstorage/

htmlfiles: htdocs
	python mkwebsite.py htdocs

htdocs:
	mkdir htdocs

#!/usr/bin/python
import publish

def createWebSite(path):
    """Create the website from various resources, and save.

    path - directory to store the created website.
    """
    nav_links = [
        ('index.html',     'Home',      'DirectoryStorage Home Page'),
        ('features.html',  'Features',  'DirectoryStorage Features'),
        ('FAQ.html',       'FAQ',       'FAQ'),
        ('install.html',   'Install',   'How to Install DirectoryStorage'),
        ('tools.html',     'Tools',     'DirectoryStorage Tools'),
        ('technical.html', 'Technical', 'Technical Documentation'),
        ('sponsor.html',   'Sponsor',   'DirectoryStorage Sponsors'),
        ]
    nav_links = '\n'.join([
        '<div class="navitem"><a href="%s" title="%s">%s</a></div>' % \
        (href, title, name) for href, name, title in nav_links
        ])

    site = publish.Website(path)
    layouter = publish.FileSimpleLayouter(
        'layout/layout.html',
        style='style.css',
        nav_links=nav_links
        )

    site.registerResources([
        publish.FileResource('layout/style.css'),
        publish.FileResource('extension.diff')],
                           '.')

    site.registerPages([
        publish.FileRstPage('index.txt'),
        publish.FileRstPage('features.txt'),
        publish.FileRstPage('FAQ.txt'),
        publish.FileRstPage('INSTALL.txt', name='install.html'),
        publish.FileRstPage('tools.txt'),
        publish.FileRstPage('technical.txt'),
        publish.FileRstPage('sponsor.txt'),

        publish.FileRstPage('backup.txt'),
        publish.FileRstPage('checkds.txt'),
        publish.FileRstPage('disaster.txt'),
        publish.FileRstPage('ds2fs.txt'),
        publish.FileRstPage('dumpdsf.txt'),
        publish.FileRstPage('fileformats.txt'),
        publish.FileRstPage('formats.txt'),
        publish.FileRstPage('keepclass.txt'),
        publish.FileRstPage('operation.txt'),
        publish.FileRstPage('rawbackup.txt'),
        publish.FileRstPage('replica.txt'),
        publish.FileRstPage('snapshot.txt'),
        ],
                       layouter, '.')
   
    site.save()

def main():
    import sys
    try:
        path = sys.argv[1]
    except IndexError:
        print "usage: mkwebsite.py website_path"
        return
    createWebSite(path)
    
if __name__ == '__main__':
    main()

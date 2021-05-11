# Various functions which implement a mapping from database name to filename.


def _bushy_munge_filename(filename):
    # convert an ordinary database filename into one which will not
    # put more than 256 files in one directory.
    i = filename.find('.')
    if i<0:
        i = len(filename)
    # If the file has a dot in it, then assume everything up to the dot
    # should be used to synthesize our bushy directories. In practice
    # this means that each oid gets its own deeply nested directory.
    tail = filename[:i]
    dir = ''
    while len(tail):
        if len(tail)>3:
            s = 2
        else:
            s = 3
        dir += tail[:s] + '/'
        tail = tail[s:]
    if filename[i+1:]:
        dir += filename[i+1:]
    else:
        dir = dir[:-1]
    return dir

assert _bushy_munge_filename('o0123456789abcdef')=='o0/12/34/56/78/9a/bc/def'
assert _bushy_munge_filename('o0123456789abcdef.c')=='o0/12/34/56/78/9a/bc/def/c'
assert _bushy_munge_filename('o0123456789abcdef.0123456789abcdef')=='o0/12/34/56/78/9a/bc/def/0123456789abcdef'
assert _bushy_munge_filename('t01234567.89abcdef')=='t0/12/34/567/89abcdef'
assert _bushy_munge_filename('x.oid')=='x/oid'

def _chunky_munge_filename(filename):
    # convert an ordinary database filename into one which works well
    # with filesystems such as reiserfs3 which support very many (2**31-k) files
    # in a directory, and many (2**16-k) subdirectories in a directory.
    i = filename.find('.')
    if i<0:
        i = len(filename)
    if i<3:
        return filename[:i]+'/'+filename[i+1:]
    else:
        # If the file has a dot in it, then assume everything up to the dot
        # can be used to synthesize our bushy directories. In practice
        # this means that each oid gets its own deeply nested directory.
        tail = filename[:i]
        emptydir = dir = ''
        while len(tail)>4:
            if dir is emptydir and tail[0] in 'ot':
                s = 4
            else:
                s = 3
            dir += tail[:s] + '/'
            tail = tail[s:]
        return dir + tail + filename[i:]
# reiserfs dilemma. If we give each oid a directory then all of its files
# get stored adjacently on disk, which makes for faster access. It also
# means that last directory is fairly empty, leading to extra directory
# traversal overhead.
assert _chunky_munge_filename('o0123456789abcdef')=='o012/345/678/9ab/cdef'
assert _chunky_munge_filename('o0123456789abcdef.c')=='o012/345/678/9ab/cdef.c'
assert _chunky_munge_filename('o0123456789abcdef.0123456789abcdef')=='o012/345/678/9ab/cdef.0123456789abcdef'
assert _chunky_munge_filename('t01234567.89abcdef')=='t012/345/67.89abcdef'
assert _chunky_munge_filename('x.oid')=='x/oid'

def _lawn_munge_filename(filename):
    # convert an ordinary database filename into one which is unlikely
    # to put too many files in one directory.
    i = filename.find('.')
    if i<0:
        # go in the database root.
        return filename
    else:
        # If the file has a dot in it, then assume everything up to the dot
        # should be used to synthesize our bushy directories. In practice
        # this means that each oid gets its own directory directly
        # off the database home
        return filename[:i]+'/'+filename[i+1:]

assert _lawn_munge_filename('o0123456789abcdef')=='o0123456789abcdef'
assert _lawn_munge_filename('o0123456789abcdef.c')=='o0123456789abcdef/c'
assert _lawn_munge_filename('o0123456789abcdef.0123456789abcdef')=='o0123456789abcdef/0123456789abcdef'
assert _lawn_munge_filename('t01234567.89abcdef')=='t01234567/89abcdef'
assert _lawn_munge_filename('x.oid')=='x/oid'


# mapping from format name to mapping function
formats = {
    'bushy':  _bushy_munge_filename,
    'chunky': _chunky_munge_filename,
    'lawn':   _lawn_munge_filename,
    'flat':   str
}

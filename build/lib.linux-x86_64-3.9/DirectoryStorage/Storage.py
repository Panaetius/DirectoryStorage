from Filesystem import Filesystem

def Storage(directory, *args, **kwargs):
    fs = Filesystem(directory)
    classname = fs.config.get('storage','classname')
    if classname == 'Full':
        from Full import Full as S
    elif classname == 'Minimal':
        from Minimal import Minimal as S
    else:
        raise ValueError('Unknown DirectoryStorage class name %r' % classname)
    return apply(S, (fs,)+args, kwargs)
    
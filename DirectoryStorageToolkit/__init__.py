import DirectoryStorageToolkit

def initialize(context):
    context.registerClass(
        DirectoryStorageToolkit.DirectoryStorageToolkit,
        constructors=(DirectoryStorageToolkit.manage_addDirectoryStorageToolkit,),
        permission='Create DirectoryStorage Toolkit',
        icon='icon.gif'
        )

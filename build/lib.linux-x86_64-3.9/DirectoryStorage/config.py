from ZODB.config import BaseConfig
 
class DirectoryStorage(BaseConfig):
    def open(self):
        from DirectoryStorage.Storage import Storage
        return Storage(self.config.path, read_only=self.config.read_only)

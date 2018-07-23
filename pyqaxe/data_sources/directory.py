import os
from .. import Cache

class Directory:
    def __init__(self, root=os.curdir, exclude_suffixes=()):
        self.root = root
        self.exclude_suffixes = set(exclude_suffixes)
        self.check_adapters()

    @classmethod
    def check_adapters(cls):
        try:
            if cls.has_registered_adapters:
                return
        except AttributeError:
            # hasn't been registered yet, run the rest of this function
            pass

        cls.has_registered_adapters = True

    def index(self, parent, conn, data_source=None, force=False):
        self.check_adapters()

        if not force:
            return

        for (dirpath, dirnames, fnames) in os.walk(self.root):
            # for dirname in dirnames:
            #     target_path = os.path.join(dirpath, dirname, '')
            #     parent.insert_file(conn, data_source, target_path)
            for fname in fnames:
                if not fname.split('.')[-1] in self.exclude_suffixes:
                    target_path = os.path.join(dirpath, fname)
                    parent.insert_file(conn, data_source, target_path)

    def __getstate__(self):
        return [self.root, self.exclude_suffixes]

    def __setstate__(self, state):
        self.__init__(*state)

    @staticmethod
    def open(filename, mode='r'):
        return open(filename, mode)

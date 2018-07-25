import os
from .. import Cache

class Directory:
    """A simple recursive directory browser.

    `Directory` populates the files table by recursively searching all
    subdirectories of a given root directory.

    :param root: Base directory to begin searching
    :param exclude_suffixes: Iterable of suffixes that should be excluded from addition to the list of files (i.e. ['txt', 'xml'])

    """
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

    def index(self, cache, conn, mine_id=None, force=False):
        self.check_adapters()

        if not force:
            return

        for (dirpath, dirnames, fnames) in os.walk(self.root, followlinks=True):
            # for dirname in dirnames:
            #     target_path = os.path.join(dirpath, dirname, '')
            #     cache.insert_file(conn, mine_id, target_path)
            for fname in fnames:
                if not fname.split('.')[-1] in self.exclude_suffixes:
                    target_path = os.path.join(dirpath, fname)
                    cache.insert_file(conn, mine_id, target_path)

    def __getstate__(self):
        return [self.root, list(sorted(self.exclude_suffixes))]

    def __setstate__(self, state):
        self.__init__(*state)

    @staticmethod
    def open(filename, mode='r'):
        return open(filename, mode)

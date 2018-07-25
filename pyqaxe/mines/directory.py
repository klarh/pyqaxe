import os
import re

class Directory:
    """A simple recursive directory browser.

    `Directory` populates the files table by recursively searching all
    subdirectories of a given root directory.

    :param root: Base directory to begin searching
    :param exclude_regexes: Iterable of regex patterns that should be excluded from addition to the list of files upon a successful search
    :param exclude_suffixes: Iterable of suffixes that should be excluded from addition to the list of files

    Examples::

        cache.index(Directory(exclude_regexes=[r'/\..*']))
        cache.index(Directory(exclude_suffixes=['txt', 'zip']))

    """
    def __init__(self, root=os.curdir, exclude_regexes=(), exclude_suffixes=()):
        self.root = root
        self.exclude_regexes = set(exclude_regexes)
        self.compiled_regexes_ = [re.compile(pat) for pat in self.exclude_regexes]
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
            for fname in fnames:
                target_path = os.path.join(dirpath, fname)
                valid = all([
                    fname.split('.')[-1] not in self.exclude_suffixes,
                    all(regex.search(target_path) is None for regex in self.compiled_regexes_)
                    ])
                if valid:
                    cache.insert_file(conn, mine_id, target_path)

    def __getstate__(self):
        return [self.root, list(sorted(self.exclude_regexes)),
                list(sorted(self.exclude_suffixes))]

    def __setstate__(self, state):
        self.__init__(*state)

    @staticmethod
    def open(filename, mode='r'):
        return open(filename, mode)

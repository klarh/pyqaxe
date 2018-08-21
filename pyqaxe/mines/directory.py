import datetime
import logging
import os
import re

try:
    from os import scandir
except ImportError:
    from scandir import scandir

from .. import Cache

logger = logging.getLogger(__name__)

class Directory:
    """A simple recursive directory browser.

    `Directory` populates the files table by recursively searching all
    subdirectories of a given root directory.

    :param root: Base directory to begin searching
    :param exclude_regexes: Iterable of regex patterns that should be excluded from addition to the list of files upon a successful search
    :param exclude_suffixes: Iterable of suffixes that should be excluded from addition to the list of files
    :param relative: Whether to store absolute or relative paths (see below)

    **Relative paths**: Directory can store relative, rather than
    absolute, paths to files. To use absolute paths, set
    `relative=False` in the constructor (default). To make the paths
    be relative to the current working directory, set
    `relative=True`. To have the paths be relative to the `Cache`
    object that indexes this mine, set `relative=cache` for that cache
    object.

    Examples::

        cache.index(Directory(exclude_regexes=[r'/\..*']))
        cache.index(Directory(exclude_suffixes=['txt', 'zip']))

    """
    def __init__(self, root=os.curdir, exclude_regexes=(), exclude_suffixes=(), relative=False):
        self.root = root
        self.exclude_regexes = set(exclude_regexes)
        self.compiled_regexes_ = [re.compile(pat) for pat in self.exclude_regexes]
        self.exclude_suffixes = set(exclude_suffixes)

        self.relative = relative
        if isinstance(relative, Cache):
            if relative.location == ':memory:':
                logger.warning('Making a Directory mine relative to a transient cache')
                self.relative_to = None
            else:
                self.relative_to = os.path.dirname(relative.location)
                self.relative = relative.unique_id
        elif relative:
            self.relative_to = os.path.abspath(os.curdir)
        else:
            self.relative_to = None

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

        if not force or cache.read_only:
            return

        directory_stack = [self.root]
        while directory_stack:
            for entry in scandir(directory_stack.pop()):
                if entry.is_dir():
                    if all(regex.search(entry.path) is None for regex in self.compiled_regexes_):
                        directory_stack.append(entry.path)
                else:
                    valid = all([
                        entry.name.split('.')[-1] not in self.exclude_suffixes,
                        all(regex.search(entry.path) is None for regex in self.compiled_regexes_)
                        ])
                    if valid:
                        path = entry.path
                        if self.relative_to:
                            path = os.path.relpath(path, self.relative_to)

                        stat = entry.stat()
                        mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
                        cache.insert_file(conn, mine_id, path, mtime, None)

    def __getstate__(self):
        return [self.root, list(sorted(self.exclude_regexes)),
                list(sorted(self.exclude_suffixes)), self.relative]

    def __setstate__(self, state):
        state = list(state)

        relative = state[3]
        if isinstance(relative, str):
            relative = Cache.get_opened_cache(relative)
            state[3] = relative

        self.__init__(*state)

    def open(self, filename, mode='r', owning_cache=None, parent=None):
        if self.relative_to:
            filename = os.path.join(self.relative_to, filename)

        return open(filename, mode)

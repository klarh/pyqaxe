import datetime
import io
import logging
import os
import re
import tarfile
import weakref

from .. import Cache

logger = logging.getLogger(__name__)

class TarFile:
    """Expose the files within one or more tar-format archives

    """

    # map opened file objects -> tarfile objects
    opened_tarfiles_ = weakref.WeakKeyDictionary()

    def __init__(self, target=None, exclude_regexes=(), exclude_suffixes=(), relative=False):
        self.exclude_regexes = set(exclude_regexes)
        self.compiled_regexes_ = [re.compile(pat) for pat in self.exclude_regexes]
        self.exclude_suffixes = set(exclude_suffixes)

        self.relative = relative
        if isinstance(relative, Cache):
            if relative.location == ':memory:':
                logger.warning('Making a TarFile mine relative to a transient cache')
                self.relative_to = None
            else:
                self.relative_to = os.path.dirname(relative.location)
                self.relative = relative.unique_id
        elif relative:
            self.relative_to = os.path.abspath(os.curdir)
        else:
            self.relative_to = None

        self.target = target
        if self.relative_to and self.target is not None:
            self.target = os.path.relpath(self.target, self.relative_to)

    def index(self, cache, conn, mine_id=None, force=False):
        if not force:
            return

        files_to_index = []
        if self.target is not None:
            target = self.target
            if self.relative_to:
                target = os.path.join(self.relative_to, target)
            stat = os.stat(target)
            mtime = datetime.datetime.fromtimestamp(stat.st_mtime)
            rowid = cache.insert_file(conn, None, target, mtime, None).lastrowid
            for row in conn.execute('SELECT rowid, * from files WHERE rowid = ?', (rowid,)):
                files_to_index.append(row)
        else:
            for row in conn.execute('SELECT rowid, * from files WHERE path LIKE "%.tar"'):
                files_to_index.append(row)

        for row in files_to_index:
            tf_id, row = row[0], row[1:]
            tf = self.get_opened_tarfile(cache, row)
            self.index_contents_(tf, cache, conn, mine_id, tf_id)

    def index_contents_(self, tf, cache, conn, mine_id, tf_id):
        for entry in tf:
            if entry.isfile():
                mtime = datetime.datetime.fromtimestamp(entry.mtime)
                cache.insert_file(conn, mine_id, entry.name, mtime, tf_id)

    def __getstate__(self):
        return [self.target, list(sorted(self.exclude_regexes)),
                list(sorted(self.exclude_suffixes)), self.relative]

    def __setstate__(self, state):
        state = list(state)

        relative = state[3]
        if isinstance(relative, str):
            relative = Cache.get_opened_cache(relative)
            state[3] = relative

        self.__init__(*state)

    @classmethod
    def get_opened_tarfile(cls, cache, row):
        opened_file = cache.open_file(row, 'rb')

        if opened_file not in cls.opened_tarfiles_:
            opened_file.seek(0)
            cls.opened_tarfiles_[opened_file] = tarfile.TarFile(fileobj=opened_file)
        return cls.opened_tarfiles_[opened_file]

    def open(self, filename, mode='r', owning_cache=None, parent=None):
        assert parent is not None

        for parent_row in owning_cache.query(
                'SELECT * FROM files WHERE rowid = ?', (parent,)):
            pass

        tf = self.get_opened_tarfile(owning_cache, parent_row)

        member = tf.getmember(filename)
        result = tf.extractfile(member)
        if 'b' not in mode:
            result = io.TextIOWrapper(result)
        return result

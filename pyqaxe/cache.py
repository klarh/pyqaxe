import datetime
import logging
import os
import pickle
import shutil
import sqlite3
import tempfile
import urllib, urllib.parse
import uuid
import weakref

logger = logging.getLogger(__name__)

from .util import LRU_Cache

class Cache:
    """A queryable cache of data found in one or more datasets

    Cache objects form the core around which the functionality of
    pyqaxe is built. They reference an sqlite database at a particular
    location; this can either be ':memory:' (default) to build an
    in-memory database or a filename to create persistent storage of
    the cached contents.

    The database is populated by indexing data sources, or *mines*,
    which may expose files for other mines to work with or create
    additional tables and associated conversion functions.

    Caches and their mines can be reconsituted in a separate process
    by simply opening a new `Cache` object pointing to the same file
    location.

    Caches can be opened in *read-only* mode which prevents
    modifications to the underlying database. Data can be selected
    from read-only databases, but indexing mines will not work.

    Caches can be used as context managers. When the context exits,
    the cache (and all of its open file handles) will be closed
    automatically.

    Cache objects create the following tables in the database:

    - mines: The data sources that have been indexed by this object
    - files: The files (or file-like objects) that have been exposed by indexed mines

    The **mines** table has the following columns:

    - pickle: A pickled representation of the mine
    - update_time: The last time the mine was indexed

    The **files** table has the following columns:

    - path: The path of the file being referenced
    - mine_id: Integer ID of the mine that provides the file

    """
    opened_caches_ = weakref.WeakValueDictionary()

    def __init__(self, location=':memory:', read_only=False):
        self.location = location

        if location == ':memory:' and read_only:
            logger.warning(
                'Opened an in-memory cache read_only, ignoring read_only flag')
            read_only = False
        self.read_only = read_only

        query_string = '?mode=ro' if read_only else ''
        location = 'file:{}{}'.format(urllib.parse.quote(location, safe=':/'), query_string)
        self.connection_ = sqlite3.connect(
            location, uri=True,
            detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.opened_file_cache_ = LRU_Cache(self.open_file_, self.close_file_, 32)

        with self.connection_ as conn:

            conn.execute('CREATE TABLE IF NOT EXISTS pyq_cache_internals '
                         '(key TEXT, value TEXT, UNIQUE (key) ON CONFLICT REPLACE)')

            self.unique_id = str(uuid.uuid4())
            for (value,) in conn.execute(
                    'SELECT value FROM pyq_cache_internals WHERE key = "unique_id"'):
                self.unique_id = value
            if not self.read_only:
                conn.execute('INSERT INTO pyq_cache_internals VALUES ("unique_id", ?)',
                             (self.unique_id,))
            self.opened_caches_[self.unique_id] = self

            conn.execute(
                'CREATE TABLE IF NOT EXISTS mines '
                '(pickle BLOB UNIQUE ON CONFLICT IGNORE, update_time TIMESTAMP)')

            # TODO add modify time of each file?
            conn.execute(
                'CREATE TABLE IF NOT EXISTS files '
                '(path TEXT, mine_id INTEGER, update_time TIMESTAMP, '
                'parent INTEGER, CONSTRAINT unique_path '
                'UNIQUE (path, mine_id, parent) ON CONFLICT REPLACE)')

            self.mines = {}
            for (rowid, pickle_data) in conn.execute(
                    'SELECT rowid, pickle from mines'):
                mine = self.mines[rowid] = pickle.loads(pickle_data)
                mine.index(self, conn, rowid, force=False)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @classmethod
    def get_opened_cache(cls, unique_id):
        """Return a currently-opened cache by its unique identifier.

        This method allows entries stored in the database to reference
        living `Cache` objects by their persistent identifier, which
        is useful for running additional queries on the database or
        retrieving opened file objects.

        """
        return cls.opened_caches_[unique_id]

    def index(self, mine, force=False):
        """Index a new mine.

        Mines may add entries to the table of files or create
        additional tables. If a mine is new to this database, it will
        be indexed regardless of the `force` argument.

        :param mine: Mine to index
        :param force: If True, force the mine to index its contents (usually implies some IO operations)
        :returns: The mine object that was indexed

        """
        with self.connection_ as conn:
            pickle_data = pickle.dumps(mine)

            conn.execute('INSERT INTO mines (pickle) VALUES (?)',
                         (pickle_data,))

            for (rowid, stored_update_time) in conn.execute(
                    'SELECT rowid, update_time FROM mines WHERE pickle = ?',
                    (pickle_data,)):
                # only one value
                pass

            if not self.read_only:
                if stored_update_time is None:
                    conn.execute('UPDATE mines SET update_time = ? WHERE rowid = ?',
                                 (datetime.datetime.fromtimestamp(0), rowid))

            self.mines[rowid] = mine

            if force or stored_update_time is None:
                begin_time = datetime.datetime.now()
                # force the first index if this source hasn't been indexed before
                mine.index(self, conn, rowid, force=True)
                if not self.read_only:
                    conn.execute('UPDATE mines SET update_time = ? WHERE rowid = ?',
                                 (begin_time, rowid))

        return mine

    def query(self, *args, **kwargs):
        """Run a query on the database.

        See :py:meth:`sqlite3.Connection.query` for details.

        """
        with self.connection_ as conn:
            for row in conn.execute(*args, **kwargs):
                yield row

    def close(self):
        """Close the connection to the database."""
        self.connection_.close()
        self.opened_file_cache_.clear()

    def insert_file(self, conn, mine_id, path, mtime=None, parent=None):
        """Insert a new entry into the files table."""
        if mtime is None:
            mtime = datetime.datetime.now()

        return conn.execute(
            'INSERT INTO files VALUES (?, ?, ?, ?)',
            (path, mine_id, mtime, parent))

    def open_file_(self, row, mode, named):
        (path, mine_id, _, parent) = row

        if mine_id is not None:
            result = self.mines[mine_id].open(path, mode, self, parent)
        else:
            result = open(path, mode)

        if named and parent is not None:
            target_fname = os.path.basename(path)
            tempfile_mode = mode.replace('r', 'w+')
            new_target = tempfile.NamedTemporaryFile(mode=tempfile_mode, suffix=target_fname)
            with result:
                shutil.copyfileobj(result, new_target)
            new_target.seek(0)
            result = new_target

        return result

    @staticmethod
    def close_file_(f):
        return f.close()

    def open_file(self, row, mode='r', named=False):
        """Open an entry from the files table.

        Pass this function an entire row from the files table, just as
        it is (i.e. `select * from files where ...`). Dispatches its
        work to the mine that owns the file. Returns a file-like
        object.

        """
        return self.opened_file_cache_(row, mode, named)

    @property
    def named_mines(self):
        """A dictionary mapping active mine type names to objects."""
        return {type(mine).__name__: mine for mine in self.mines.values()}

    @property
    def ordered_mines(self):
        """A list of each active mine, in order of indexing."""
        return [self.mines[key] for key in sorted(self.mines)]

    def get_cache_size(self):
        """Return the maximumnumber of files to keep open."""
        return self.opened_file_cache_.max_size

    def set_cache_size(self, value):
        """Set the maximum number of files to keep open."""
        self.opened_file_cache_.max_size = value

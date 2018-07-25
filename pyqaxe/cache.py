from collections import namedtuple
import datetime
import pickle
import sqlite3
import json
import re
import weakref
import hashlib

class Cache:
    """A queryable cache of data found in one or more datasets

    Cache objects form the core around which the functionality of
    pyqaxe is built. They reference an sqlite database at a particular
    location; this can either be ':memory:' (default) to build an
    in-memory database or a filename to create persistent storage of
    the cached contents.

    The database is populated by *indexing* data sources, which may
    expose files for other data sources to work with or create
    additional tables and associated conversion functions.

    Caches and their data sources can be reconsituted in a separate
    process by simply opening a new `Cache` object pointing to the
    same file location.

    Cache objects create the following tables in the database:

    - data_sources: The data sources that have been indexed by this object
    - files: The files (or file-like objects) that have been exposed by other data sources

    The **data_sources** table has the following columns:

    - pickle: A pickled representation of the data source
    - update_time: The last time the data source was indexed

    The **files** table has the following columns:

    - path: The path of the file being referenced
    - data_source: Integer ID of the data source that provides the file

    """
    opened_caches_ = weakref.WeakValueDictionary()

    def __init__(self, location=':memory:'):
        self.connection_ = sqlite3.connect(
            location, detect_types=sqlite3.PARSE_DECLTYPES)

        unique_location = location
        if location == ':memory:':
            while unique_location in self.opened_caches_:
                match = re.search(r'(\d+)$', unique_location)
                index = 1 if match is none else int(match.groups()[0])
                unique_location = ':memory:{}'.format(index + 1)
        self.unique_id = hashlib.sha256(unique_location.encode('UTF-8')).hexdigest()
        self.opened_caches_[self.unique_id] = self

        with self.connection_ as conn:
            conn.execute(
                'CREATE TABLE IF NOT EXISTS data_sources '
                '(pickle BLOB UNIQUE ON CONFLICT IGNORE, update_time DATETIME)')

            # TODO add modify time of each file?
            conn.execute(
                'CREATE TABLE IF NOT EXISTS files '
                '(path TEXT, data_source INTEGER, '
                'CONSTRAINT unique_path '
                'UNIQUE (path, data_source) ON CONFLICT IGNORE)')

            self.data_sources = {}
            for (rowid, pickle_data) in conn.execute(
                    'SELECT rowid, pickle from data_sources'):
                data_source = self.data_sources[rowid] = pickle.loads(pickle_data)
                data_source.index(self, conn, rowid, force=False)

    @classmethod
    def get_opened_cache(cls, unique_id):
        """Return a currently-opened cache by its unique identifier.

        This method allows entries stored in the database to reference
        living `Cache` objects by their persistent identifier, which
        is useful for running additional queries on the database or
        retrieving opened file objects.

        """
        return cls.opened_caches_[unique_id]

    def index(self, data_source, force=False):
        """Index a new data source.

        Data sources may add entries to the table of files or create
        additional tables. If a data source is new to this database,
        it will be indexed regardless of the `force` argument.

        :param data_source: Data source to index
        :param force: If True, force the data source to index its contents (usually implies some IO operations)

        """
        with self.connection_ as conn:
            cursor = conn.cursor()
            pickle_data = pickle.dumps(data_source)

            cursor.execute('INSERT INTO data_sources (pickle) VALUES (?)',
                         (pickle_data,))

            cursor.execute('SELECT rowid, update_time FROM data_sources WHERE pickle = ?',
                         (pickle_data,))
            (rowid, stored_update_time) = cursor.fetchone()

            if force or stored_update_time is None:
                begin_time = datetime.datetime.now()
                # force the first index if this source hasn't been indexed before
                data_source.index(self, cursor, rowid, force=True)
                cursor.execute('UPDATE data_sources SET update_time = ? WHERE rowid = ?',
                             (begin_time, rowid))

            self.data_sources[rowid] = data_source

    def query(self, *args, **kwargs):
        """Run a query on the database.

        See :py:meth:`sqlite3.Connection.query` for details.

        """
        with self.connection_ as conn:
            return conn.execute(*args, **kwargs)

    def close(self):
        """Close the connection to the database."""
        self.connection_.close()

    def insert_file(self, conn, data_source, path):
        """Insert a new entry into the files table."""
        return conn.execute(
            'INSERT INTO files VALUES (?, ?)',
            (path, data_source))

    def open_file(self, row, mode='r'):
        """Open an entry from the files table.

        Pass this function an entire row from the files table, just as
        it is (i.e. `select * from files where ...`). Dispatches its
        work to the data source that owns the file. Returns a
        file-like object.

        """
        (path, data_source) = row
        return self.data_sources[data_source].open(path, mode)

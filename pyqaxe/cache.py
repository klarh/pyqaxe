from collections import namedtuple
import datetime
import pickle
import sqlite3
import json
import re
import weakref
import hashlib

class Cache:
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
        return cls.opened_caches_[unique_id]

    def index(self, data_source, force=False):
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
        with self.connection_ as conn:
            return conn.execute(*args, **kwargs)

    def close(self):
        self.connection_.close()

    def insert_file(self, conn, data_source, path):
        return conn.execute(
            'INSERT INTO files VALUES (?, ?)',
            (path, data_source))

    def open_file(self, row, mode='r'):
        (path, data_source) = row
        return self.data_sources[data_source].open(path, mode)

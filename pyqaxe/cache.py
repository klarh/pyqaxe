from collections import namedtuple
import datetime
import pickle
import sqlite3
import json
import re

class Cache:
    def __init__(self, location=':memory:'):
        self.connection_ = sqlite3.connect(
            location, detect_types=sqlite3.PARSE_DECLTYPES)

        with self.connection_ as conn:
            conn.execute(
                'CREATE TABLE IF NOT EXISTS data_sources '
                '(pickle BLOB UNIQUE ON CONFLICT IGNORE, update_time DATETIME)')

            conn.execute(
                'CREATE TABLE IF NOT EXISTS files '
                '(path TEXT, data_source INTEGER, '
                'CONSTRAINT unique_path '
                'UNIQUE (path, data_source) ON CONFLICT IGNORE)')

            data_sources = {}
            for (rowid, pickle_data) in conn.execute(
                    'SELECT rowid, pickle from data_sources'):
                data_source = data_sources[rowid] = pickle.loads(pickle_data)
                data_source.index(self, conn, rowid, force=False)

        self.data_sources = data_sources

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
                data_source.index(self, cursor, rowid, force=force)
                cursor.execute('UPDATE data_sources SET update_time = ? WHERE rowid = ?',
                             (begin_time, rowid))

            self.data_sources[rowid] = data_source

    def query(self, query):
        with self.connection_ as conn:
            return conn.execute(query)

    def close(self):
        self.connection_.close()

    def insert_file(self, conn, data_source, path):
        return conn.execute(
            'INSERT INTO files VALUES (?, ?)',
            (path, data_source))

    def open_file(self, row, mode='r'):
        (path, data_source) = row
        return self.data_sources[data_source].open(path, mode)

import gtar
import json
import sqlite3
from .. import Cache

def encode_gtar_data(path, file_id, cache_id):
    return json.dumps([path, file_id, cache_id]).encode('UTF-8')

def convert_gtar_data(contents):
    (path, file_id, cache_id) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    # TODO use a cache to save on re-opening files each time
    with cache.open_file(row, 'rb') as f:
        with gtar.GTAR(f.name, 'r') as traj:
            return traj.readPath(path)

class GTAR:
    def __init__(self):
        pass

    def index(self, cache, conn, data_source=None, force=False):
        self.check_adapters()

        conn.execute('CREATE TABLE IF NOT EXISTS gtar_records '
                     '(path TEXT, gtar_group TEXT, gtar_index TEXT, name TEXT, '
                     'file_id INTEGER, cache_id TEXT, data GTAR_DATA, '
                     'CONSTRAINT unique_gtar_path '
                     'UNIQUE (path, file_id, cache_id) ON CONFLICT IGNORE)')

        # don't do file IO if we aren't forced
        if not force:
            return

        for row in conn.execute(
                'SELECT rowid, * from files WHERE path LIKE "%.zip" OR '
                'path LIKE "%.tar" OR path LIKE "%.sqlite"'):
            file_id = row[0]
            row = row[1:]
            # TODO use a cache to save on re-opening files each time
            with cache.open_file(row, 'rb') as f:
                with gtar.GTAR(f.name, 'r') as traj:
                    for record in traj.getRecordTypes():
                        group = record.getGroup()
                        name = record.getName()
                        for frame in traj.queryFrames(record):
                            record.setIndex(frame)
                            path = record.getPath()

                            encoded_data = encode_gtar_data(
                                path, file_id, cache.unique_id)
                            values = (path, group, frame, name, file_id,
                                      cache.unique_id, encoded_data)
                            conn.execute(
                                'INSERT INTO gtar_records VALUES (?, ?, ?, ?, ?, ?, ?)',
                                values)
        pass

    @classmethod
    def check_adapters(cls):
        try:
            if cls.has_registered_adapters:
                return
        except AttributeError:
            # hasn't been registered yet, run the rest of this function
            pass

        sqlite3.register_converter('GTAR_DATA', convert_gtar_data)
        cls.has_registered_adapters = True

    def __getstate__(self):
        return []

    def __setstate__(self, state):
        self.__init__(*state)

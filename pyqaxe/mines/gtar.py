import gtar
import json
import logging
import re
import sqlite3
from .. import Cache, util

logger = logging.getLogger(__name__)

def open_gtar(cache_id, file_row):
    cache = Cache.get_opened_cache(cache_id)
    opened_file = cache.open_file(file_row, 'rb', named=True)
    gtar_traj = gtar.GTAR(opened_file.name, 'r')
    return (opened_file, gtar_traj)

def close_gtar(args):
    (opened_file, gtar_traj) = args
    gtar_traj.close()
    opened_file.close()

def encode_gtar_data(path, file_id, cache_id):
    return json.dumps([path, file_id, cache_id]).encode('UTF-8')

def convert_gtar_data(contents):
    (path, file_id, cache_id) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    (_, traj) = GTAR.opened_trajectories_(cache_id, row)
    return traj.readPath(path)

def collate_gtar_index(left, right):
    left = (len(left), left)
    right = (len(right), right)

    if left < right:
        return -1
    elif left == right:
        return 0
    return 1

class GTAR:
    """Interpret getar-format files.

    `GTAR` parses zip, tar, and sqlite-format archives in the getar
    format (https://libgetar.readthedocs.io) to expose trajectory
    data. The getar files themselves are opened upon indexing to find
    which records are available in each file, but the actual data
    contents are read on-demand.

    :param exclude_frames_regexes: Iterable of regex patterns of quantity names that should be excluded as columns from `gtar_frames` table (see below)

    GTAR objects create the following table in the database:

    - gtar_records: Contains links to data found in all getar-format files
    - gtar_frames: Contains sets of data stored by index for all getar-format files

    The **gtar_records** table has the following columns:

    - path: path within the archive of the record
    - gtar_group: *group* for the record
    - gtar_index: *index* for the record
    - name: *name* for the record
    - file_id: files table identifier for the archive containing this record
    - data: exposes the data of the record. Value is a string, bytes, or array-like object depending on the stored format.

    The **gtar_frames** table's columns depend on which records are
    found among the indexed files. For each unique index, it lists all
    quantities found among all archives as columns (note that some
    quantity names may need to be surrounded by quotes) up to that
    index. gtar_frames contains the following additional columns:

    - gtar_index: *index* for the record
    - file_id: files table identifier for the archive containing this record

    :
        cache.query('SELECT box, position FROM gtar_frames')

    GTAR objects register a **gtar_frame** collation that can be used
    to sort indices in the standard GTAR way, rather than sqlite's
    default string comparison::

        cache.query('SELECT data FROM gtar_records WHERE name = "position" '
                    'ORDER BY gtar_index COLLATE gtar_frame')

    .. note::
        Consult the libgetar documentation to find more details about
        how records are encoded.

    """
    opened_trajectories_ = util.LRU_Cache(open_gtar, close_gtar, 16)
    GTAR_FRAMES_COLUMN_WARNING = 128
    GTAR_FRAMES_COLUMN_SKIP = 512

    def __init__(self, exclude_frames_regexes=(r'\.',)):
        self.exclude_frames_regexes = set(exclude_frames_regexes)
        self.compiled_frames_regexes_ = [re.compile(pat) for pat in self.exclude_frames_regexes]

    def index(self, cache, conn, mine_id=None, force=False):
        self.check_adapters()

        conn.create_collation('gtar_frame', collate_gtar_index)

        conn.execute('CREATE TABLE IF NOT EXISTS gtar_records '
                     '(path TEXT, gtar_group TEXT, gtar_index TEXT, name TEXT, '
                     'file_id INTEGER, data GTAR_DATA, '
                     'CONSTRAINT unique_gtar_path '
                     'UNIQUE (path, file_id) ON CONFLICT IGNORE)')

        # don't do file IO if we aren't forced
        if not force or cache.read_only:
            return

        for (mine_update_time,) in conn.execute(
                'SELECT update_time FROM mines WHERE rowid = ?',
                (mine_id,)):
            pass

        # all rows to insert into glotzformats_frames (TODO interleave
        # reading and writing if size of all_values becomes an issue)
        all_values = []
        for row in conn.execute(
                'SELECT rowid, * from files WHERE (update_time > ?) AND '
                '(path LIKE "%.zip" OR path LIKE "%.tar" OR path LIKE "%.sqlite")',
                (mine_update_time,)):
            file_id = row[0]
            row = row[1:]

            try:
                (_, traj) = GTAR.opened_trajectories_(cache.unique_id, row)
            except RuntimeError as e:
                # gtar library throws RuntimeErrors when archives are
                # corrupted, for example; skip this one with a warning
                logger.warning('{}: {}'.format(row[0], e))
                continue

            for record in traj.getRecordTypes():
                group = record.getGroup()
                name = record.getName()
                for frame in traj.queryFrames(record):
                    record.setIndex(frame)
                    path = record.getPath()

                    encoded_data = encode_gtar_data(
                        path, file_id, cache.unique_id)
                    values = (path, group, frame, name, file_id, encoded_data)
                    all_values.append(values)

        for values in all_values:
            conn.execute(
                'INSERT INTO gtar_records VALUES (?, ?, ?, ?, ?, ?)', values)

        conn.execute('DROP TABLE IF EXISTS gtar_frames')

        all_names = list(sorted(row[0] for row in conn.execute(
            'SELECT DISTINCT name FROM gtar_records') if
            all(pat.search(row[0]) is None for pat in self.compiled_frames_regexes_)))
        column_defs = ['file_id INTEGER', 'gtar_group TEXT', 'gtar_index TEXT']
        column_defs.extend([
            '"{name}" GTAR_DATA'.format(name=name) for name in all_names
        ])

        if len(all_names) > self.GTAR_FRAMES_COLUMN_SKIP:
            logger.warning('Attempting to create {} columns in gtar_frames, '
                           'skipping instead'.format(len(all_names)))
            return
        elif len(all_names) > self.GTAR_FRAMES_COLUMN_WARNING:
            logger.warning('Creating {} columns in gtar_frames'.format(len(all_names)))

        query = ('CREATE TABLE gtar_frames ({})').format(
            ', '.join(column_defs))
        conn.execute(query)

        name_column_indices = {name: i for (i, name) in enumerate(all_names)}
        all_values = []
        last_fileid_group_index = (None, None, None)
        current_row = [None]*len(all_names)
        for (fileid, group, index, name, data) in cache.query(
                # pass data through a function to make the record stay
                # as a bytestring rather than being automatically read
                'SELECT file_id, gtar_group, gtar_index, name, likely(data) FROM '
                'gtar_records ORDER BY file_id, gtar_group, gtar_index COLLATE gtar_frame'):

            fileid_group_index = (fileid, group, index)
            if (fileid_group_index != last_fileid_group_index and
                any(val is not None for val in current_row)):

                all_values.append((last_fileid_group_index, list(current_row)))
                if fileid_group_index[:2] != last_fileid_group_index[:2]:
                    current_row = [None]*len(all_names)

            last_fileid_group_index = fileid_group_index
            if name in name_column_indices:
                current_row[name_column_indices[name]] = data
        if any(val is not None for val in current_row):
            all_values.append((last_fileid_group_index, current_row))

        query = 'INSERT INTO gtar_frames VALUES ({})'.format(
            ', '.join((len(last_fileid_group_index) + len(all_names))*'?'))
        for (ids, rest_of_row) in all_values:
            conn.execute(query, list(ids) + list(rest_of_row))

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
        return [list(sorted(self.exclude_frames_regexes))]

    def __setstate__(self, state):
        self.__init__(*state)

    @classmethod
    def get_cache_size(cls):
        """Return the maximum number of files to keep open."""
        return cls.opened_trajectories_.max_size

    @classmethod
    def set_cache_size(cls, value):
        """Set the maximum number of files to keep open."""
        cls.opened_trajectories_.max_size = value

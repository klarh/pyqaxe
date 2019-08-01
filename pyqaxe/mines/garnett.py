import garnett
import json
import logging
import re
import sqlite3
import weakref
from .. import Cache

logger = logging.getLogger(__name__)

def encode_garnett_data(file_id, cache_id, frame, attribute):
    return json.dumps([file_id, cache_id, frame, attribute]).encode('UTF-8')

def convert_garnett_data(contents):
    (file_id, cache_id, frame, attribute) = json.loads(contents.decode('UTF-8'))
    cache = Cache.get_opened_cache(cache_id)
    for row in cache.query('SELECT * from files WHERE rowid = ?', (file_id,)):
        # set row for open_file below
        pass

    suffix = row[0].split('.')[-1]

    trajectory = Garnett.get_opened_trajectory(cache, row, suffix)
    return getattr(trajectory[frame], attribute)

class Garnett:
    """Expose frames of garnett-readable trajectory formats.

    `Garnett` parses trajectory files and exposes them with a
    common interface. Files are opened once upon indexing to query the
    number of frames and data are read on-demand as frame data are
    selected.

    Garnett objects create the following table in the database:

    - garnett_frames: Contains entries for each frame found in all trajectory files

    The **garnett_frames** table has the following columns:

    - file_id: files table identifier for the archive containing this record
    - cache_id: `Cache` unique identifier for the archive containing this record
    - frame: Integer (0-based) corresponding to the frame index within the trajectory
    - box: Garnett box object for the frame
    - types: Garnett types object for the frame
    - positions: Garnett positions object for the frame
    - velocities: Garnett velocities object for the frame
    - orientations: Garnett orientations object for the frame
    - shapedef: Garnett shapedef object for the frame

    .. note::
        Consult the garnett documentation to find more details
        about the encoding of the various data types listed here.

    """
    opened_trajectories_ = weakref.WeakKeyDictionary()

    binary_formats = {'zip', 'tar', 'sqlite', 'gsd'}

    # formats that need a named file to exist somewhere to read
    named_formats = {'zip', 'tar', 'sqlite'}

    readers = dict(
        zip=garnett.reader.GetarFileReader,
        tar=garnett.reader.GetarFileReader,
        sqlite=garnett.reader.GetarFileReader,
        pos=garnett.reader.PosFileReader,
        gsd=garnett.reader.GSDHOOMDFileReader
        )

    known_frame_attributes = ['box', 'types', 'positions', 'velocities',
                              'orientations', 'shapedef']

    def __init__(self, exclude_regexes=(), exclude_suffixes=()):
        self.exclude_regexes = set(exclude_regexes)
        self.compiled_regexes_ = [re.compile(pat) for pat in self.exclude_regexes]
        self.exclude_suffixes = set(exclude_suffixes)

    def index(self, cache, conn, mine_id=None, force=False):
        self.check_adapters()

        all_attributes = ', '.join(
            ['{} GARNETT_{}'.format(attr, attr)
             for attr in self.known_frame_attributes])
        query = ('CREATE TABLE IF NOT EXISTS garnett_frames '
                 '(file_id INTEGER, '
                 'frame INTEGER, {attributes}, '
                 'CONSTRAINT unique_garnett_path '
                 'UNIQUE (file_id, frame) ON CONFLICT IGNORE)').format(
                     attributes=all_attributes)
        conn.execute(query)

        # don't do file IO if we aren't forced
        if not force or cache.read_only:
            return

        for (mine_update_time,) in conn.execute(
                'SELECT update_time FROM mines WHERE rowid = ?',
                (mine_id,)):
            pass

        insert_query = 'INSERT INTO garnett_frames VALUES ({})'.format(
            ', '.join((len(self.known_frame_attributes) + 2)*'?'))
        for row in conn.execute(
                'SELECT rowid, * from files WHERE (update_time > ?) AND '
                '(path LIKE "%.zip" OR '
                'path LIKE "%.tar" OR path LIKE "%.sqlite" OR '
                'path LIKE "%.pos" OR path LIKE "%.gsd")',
                (mine_update_time,)):
            file_id, row = row[0], row[1:]
            path = row[0]
            suffix = path.split('.')[-1]

            valid = all([
                suffix not in self.exclude_suffixes,
                all(regex.search(path) is None for regex in self.compiled_regexes_)
                ])
            if not valid:
                continue

            try:
                trajectory = self.get_opened_trajectory(cache, row, suffix)
            except garnett.errors.ParserError as e:
                logger.warning('{}: {}'.format(row[0], e))
                continue
            except RuntimeError as e:
                # gtar library throws RuntimeErrors when archives are
                # corrupted, for example; skip this one with a warning
                logger.warning('{}: {}'.format(row[0], e))
                continue

            for frame in range(len(trajectory)):
                values = [file_id, frame]
                for attr in self.known_frame_attributes:
                    values.append(encode_garnett_data(file_id, cache.unique_id, frame, attr))
                conn.execute(insert_query, values)

    @classmethod
    def check_adapters(cls):
        try:
            if cls.has_registered_adapters:
                return
        except AttributeError:
            # hasn't been registered yet, run the rest of this function
            pass

        for attr in cls.known_frame_attributes:
            upper_name = 'GARNETT_{}'.format(attr.upper())
            sqlite3.register_converter(upper_name, convert_garnett_data)
        cls.has_registered_adapters = True

    def __getstate__(self):
        return [list(sorted(self.exclude_regexes)),
                list(sorted(self.exclude_suffixes))]

    def __setstate__(self, state):
        self.__init__(*state)

    @classmethod
    def get_opened_trajectory(cls, cache, row, suffix):
        open_mode = 'rb' if suffix in Garnett.binary_formats else 'r'
        named = suffix in cls.named_formats
        opened_file = cache.open_file(row, open_mode, named=named)

        if opened_file not in cls.opened_trajectories_:
            opened_file.seek(0)
            cls.opened_trajectories_[opened_file] = Garnett.readers[suffix]().read(opened_file)

        return cls.opened_trajectories_[opened_file]

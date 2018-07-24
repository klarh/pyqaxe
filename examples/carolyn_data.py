import pyqaxe as pyq
from pyqaxe.data_sources.glotzformats import GlotzFormats

c = pyq.Cache('/tmp/carolyn_cache.sqlite')

c.index(pyq.data_sources.Directory(
    '/nfs/glotzer/projects/potential-zoo/carolynDataFromDropbox/ModMarek/raw/pos_data'))
c.index(GlotzFormats())

for (count,) in c.query('select count(distinct(file_id)) from glotzformats_frames'):
    print('{} unique files'.format(count))

c.close()

c2 = pyq.Cache('/tmp/carolyn_cache.sqlite')

for (count,) in c2.query('select count(*) from glotzformats_frames'):
    print('{} total frames'.format(count))

for (frame, positions, orientations) in c2.query(
        'select frame, positions, orientations from glotzformats_frames limit 5'):
    print(frame, positions[0], orientations.shape)

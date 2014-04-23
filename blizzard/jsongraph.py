import json
import functools
from itertools import combinations

def write_triple(fp, subject, predicate, object):
    row = (subject, predicate, object)
    fp.write(json.dumps(row) + '\n')

def add_dataset(metadata, fp, dataset):
    '''
    overlap  :: dataset text -> dict
    fp       :: file-like object
    dataset  :: dict
    '''
    dataset_url = 'http://%s/explore/dataset/%s/' % (dataset['catalog'], dataset['datasetid'])
    self.add_node(dataset_id, kind = 'dataset')
    save = functools.partial(write_triple, fp, dataset_url)
    for unique_index in dataset['unique_indices']:
        i = tuple(sorted(unique_index))
        save('unique index', i)
    for k, v in metadata(dataset['download'].text):
        save(k, v)

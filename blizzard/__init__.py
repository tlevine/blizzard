import functools
import pickle
from io import StringIO
import os

from blizzard.util import _get, ignore
from blizzard.graph import Graph
import blizzard.download as download

def main():
    n_columns = 3
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/opendatasoft')

    g = Graph()
    get = functools.partial(_get, datadir)
    for dataset in download.all(get):
        response = download.download(get, dataset['catalog'], dataset['datasetid'])
        if not ignore(dataset):
            with StringIO(response.text) as fp:
                dataset['unique_indices'] = fromcsv(fp, delimiter = ';', n_columns = n_columns)
            g.add_dataset(dataset)
    with open('graph.p', 'wb') as fp:
        pickle.dump(g, fp)

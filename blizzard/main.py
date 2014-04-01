import functools
import pickle
from io import StringIO
import os

import pickle_warehouse

from blizzard.graph import Graph
import blizzard.download as download

def _get(datadir:str, load:bool, url:str):
    warehouse = pickle_warehouse.Warehouse(datadir)
    if url in warehouse:
        response = warehouse[url]
    else:
        response = requests.get(url)
        warehouse[url] = response
    return response

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

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

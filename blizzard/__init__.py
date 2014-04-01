import functools
from concurrent.futures import ThreadPoolExecutor
import pickle
from io import StringIO
import os
import logging

from special_snowflake import fromcsv

from blizzard.util import _get, ignore
from blizzard.graph import Graph
import blizzard.download as download

logger = logging.getLogger('blizzard')

def main():
    n_columns = 3
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/opendatasoft')
    n_workers = 30

    fp_log = logging.FileHandler('blizzard.log', 'a')
    logger.setLevel(logging.DEBUG)
    fp_log.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)

    logger.debug('Starting a new run\n==============================================')
    g = Graph()
    get = functools.partial(_get, datadir)
    with ThreadPoolExecutor(n_workers) as e:
        for dataset in e.map(_snow, download.all(get)):
            g.add_dataset(dataset)
    with open('graph.p', 'wb') as fp:
        pickle.dump(g, fp)

def _snow(dataset):
    dataset = dict(dataset)
    logger.debug('%s, %s: Got dataset' % (dataset['catalog'], dataset['datasetid']))
    if ignore(dataset):
        logger.debug('%s, %s: Skipping' % (dataset['catalog'], dataset['datasetid']))
    else:
        logger.debug('%s, %s: Snowflaking' % (dataset['catalog'], dataset['datasetid']))
        with StringIO(dataset['download'].text) as fp:
            try:
                dataset['unique_indices'] = fromcsv(fp, delimiter = ';', n_columns = n_columns)
            except:
                logger.error('%s, %s: Error' % (dataset['catalog'], dataset['datasetid']))
                dataset['unique_indices'] = set()

        logger.debug('%s, %s: Found indices %s' % (dataset['catalog'], dataset['datasetid'], dataset['unique_indices']))
    return dataset

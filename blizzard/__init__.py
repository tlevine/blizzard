import functools
import pickle
from io import StringIO
import os
import logging

from special_snowflake import fromcsv

from blizzard.util import _get, ignore
from blizzard.graph import Graph
import blizzard.download as download

def main():
    n_columns = 3
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/opendatasoft')

    logger = logging.getLogger('blizzard')
    fp_log = logging.FileHandler('blizzard.log', 'a')
    logger.setLevel(logging.DEBUG)
    fp_log.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)

    logger.debug('Starting a new run\n==============================================')
    g = Graph()
    get = functools.partial(_get, datadir)
    for dataset in download.all(get):
        logger.debug('%s, %s: Got metadata' % (dataset['catalog'], dataset['datasetid']))
        logger.debug('%s, %s: Got spreadsheet' % (dataset['catalog'], dataset['datasetid']))

        if ignore(dataset):
            logger.debug('%s, %s: Skipping' % (dataset['catalog'], dataset['datasetid']))
        else:
            logger.debug('%s, %s: Snowflaking' % (dataset['catalog'], dataset['datasetid']))
            with StringIO(dataset['download'].text) as fp:
                try:
                    dataset['unique_indices'] = fromcsv(fp, delimiter = ';', n_columns = n_columns)
                except:
                    logger.error('%s, %s: Error' % (dataset['catalog'], dataset['id']))
                    dataset['unique_indices'] = set()

            logger.debug('%s, %s: Found indices %s' % (dataset['catalog'], dataset['datasetid'], dataset['unique_indices']))
            g.add_dataset(dataset)

    with open('graph.p', 'wb') as fp:
        pickle.dump(g, fp)

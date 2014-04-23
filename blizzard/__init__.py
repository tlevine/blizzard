import functools
import sys
import pickle
from io import StringIO
import os
import logging
from more_itertools import ilen

from jumble import jumble
from special_snowflake import fromcsv

from blizzard.util import _get, ignore
import blizzard.download as download
from blizzard.jsongraph import add_dataset as _add_dataset

logger = logging.getLogger('blizzard')
formatter = logging.Formatter()

__version__ = '0.0.1'

def main():
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/opendatasoft')
    n_workers = 30

    fp_log = logging.FileHandler('blizzard.log', 'a')
    logger.setLevel(logging.DEBUG)
    fp_log.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)

    logger.debug('Starting a new run\n==============================================')
    get = functools.partial(_get, datadir)

    fp = open('blizzard.jsonlines', 'w')
    add_dataset = functools.partial(_add_dataset, fp)

    for future in jumble(_snow, download.all(get)):
        dataset = future.result()
        if dataset != None:
            logger.debug('%s, %s: Saving %s' % (dataset['catalog'], dataset['datasetid'], dataset))
            add_dataset(dataset)

    fp.close()

def _snow(dataset):
    n_columns = 3
    dataset = dict(dataset)
    logger.debug('%s, %s: Got dataset' % (dataset['catalog'], dataset['datasetid']))
    if ignore(dataset):
        logger.debug('%s, %s: Skipping geographic data' % (dataset['catalog'], dataset['datasetid']))
        dataset['unique_indices'] = set()
    elif not dataset['download'].ok:
        args = (dataset['catalog'], dataset['datasetid'], dataset['download'].status_code)
        logger.debug('%s, %s: Skipping status code %d' % args)
        dataset['unique_indices'] = set()
    else:
        logger.debug('%s, %s: Snowflaking' % (dataset['catalog'], dataset['datasetid']))
        with StringIO(dataset['download'].text) as fp:
            try:
                dataset['unique_indices'] = fromcsv(fp, delimiter = ';', n_columns = n_columns)
            except:
                tb = formatter.formatException(sys.exc_info())
                logger.error('%s, %s:\n%s\n\n' % (dataset['catalog'], dataset['datasetid'], tb))
                dataset['unique_indices'] = set()

        logger.debug('%s, %s: Found indices %s' % (dataset['catalog'], dataset['datasetid'], dataset['unique_indices']))
    return dataset

def nrow_overlap(index, a,b):
    sorted_index = sort(tuple(index))
    def select(text):
        with StringIO(text) as fp:
            r = csv.DictReader(fp, delimiter = ';')
            for i, row in r:
                yield i, tuple(row[col] for col in sorted_index)

    def getdata(selections):
        data = {'nrow':None,'values':set()}
        for nrow, values in selections:
            data['nrow'] = nrow
            data['values'].add(values)
        return data

    data_a = getdata(select(a))
    data_b = getdata(select(b))
    node_attrs = {a: data_a['nrow'],b: data_b['nrow']},  
    edge_attrs = {(a,b): len(data_a['values'].intersection(data_b['values']))}
    return node_attrs, edge_attrs

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    return {'nrow': nrow}

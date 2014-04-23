import json
import logging
import functools
from io import StringIO
import os
import csv
from concurrent.futures import ProcessPoolExecutor
import sys

from more_itertools import ilen
from special_snowflake import fromcsv

from blizzard.util import _get, ignore
import blizzard.download as dl
from blizzard.nxgraph import Graph

def main():
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/big/opendatasoft')
    n_workers = 30

    # Logging
    fp_log = logging.FileHandler('blizzard.log', 'w')
    fp_log.setLevel(logging.DEBUG)
    logger = logging.getLogger('blizzard')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)
    logger.debug('Starting a new run\n==============================================')

    get = functools.partial(_get, datadir)

    with ProcessPoolExecutor(4) as e:
        for catalog in dl.catalogs:
            for dataset in dl.datasets(get, catalog):
                dataset['download'] = dl.download(get, catalog, dataset['datasetid'])
                if not ignore(dataset):
                    e.submit(snowflake, dataset)

def snowflake(dataset):
    dataset.update(metadata(dataset['download'].text))
    del(dataset['download'])
    sys.stdout.write(json.dumps(dataset) + '\n')

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    with StringIO(dataset_text) as fp:
        unique_keys = list(sorted(fromcsv(fp, delimiter = ';')))
    return {'nrow': nrow, 'unique_keys': unique_keys}

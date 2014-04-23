import logging
import functools
from io import StringIO
import os
import csv

from more_itertools import ilen
from jumble import jumble
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

#   dataset = dl.download(get, 'http://data.iledefrance.fr', 'etudiants_boursiers_des_formations_sanitaires_et_sociales_sept_2007')
#   print(metadata(dataset.text))

    for catalog in dl.catalogs:
        def f(dataset):
            dataset['download'] = dl.download(get, catalog, dataset['datasetid'])
            return dataset
        for future in jumble(f, datasets(get, catalog), n_workers):
            dataset = future.result()
            dataset.update(metadata(dataset['download'].text))
            del(dataset['download'])
            print(json.dumps(dataset))

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    return {'nrow': nrow}

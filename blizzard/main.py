import logging
import functools
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor

import blizzard.util as u
import blizzard.meta as meta
import blizzard.download as dl
from blizzard.nxgraph import Graph

logger = logging.getLogger('blizzard')

def main():
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/big/opendatasoft')
    n_workers = 30

    # Logging
    fp_log = logging.FileHandler('blizzard.log', 'w')
    fp_log.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)
    logger.debug('Starting a new run\n==============================================')

    get = u.downloader(datadir)
    command = u.parser().parse_args().command
    if command == 'index':
        index(get, sys.stdout)
    elif command == 'graph':
        graph(get, sys.stdin, sys.stdout)

def index(get, fp_out):
    futures = {}
    with ProcessPoolExecutor(4) as e:
        for catalog in dl.catalogs:
            for dataset in dl.datasets(get, catalog):
                logger.debug(u.dataset_download_url(dataset['catalog'], dataset['datasetid']))
                dataset['download'] = dl.download(get, catalog, dataset['datasetid'])
                if not u.ignore(dataset):
                    futures[(dataset['catalog'], dataset['datasetid'])] = e.submit(meta.snowflake, dataset)

        while futures != {}:
            for key, future in list(futures.items()):
                if future.done():
                    dataset = future.result()
                    fp_out.write(json.dumps(dataset) + '\n')
                    del(futures[key])
                    logger.debug('Remaining datasets:\n%s' % futures.keys())

def graph(get, fp_in, fp_out):
    g = Graph()
    for line in fp_in:
        g.add_dataset(json.loads(line))
    for left, right in g.similarly_indexed_datasets():
        fp_out.write(u.dataset_url(*left) + '  <-->  ' + u.dataset_url(*right) + '\n')

import logging
import functools
import os
import sys
from concurrent.futures import ProcessPoolExecutor
import argparse

from blizzard.util import _get, ignore
import blizzard.download as dl
from blizzard.nxgraph import Graph, dataset_url

def parser():
    p = argparse.ArgumentParser()
    p.add_argument('command', choices = ['index', 'graph'])
    return p

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
    command = parser().parse_args().command
    if command == 'index':
        index(get, sys.stdout)
    elif command == 'graph':
        h

def index(get, fp_out):
    with ProcessPoolExecutor(4) as e:
        for catalog in dl.catalogs:
            for dataset in dl.datasets(get, catalog):
                dataset['download'] = dl.download(get, catalog, dataset['datasetid'])
                if not ignore(dataset):
                    e.submit(functools.partial(snowflake, fp_out), dataset)

def graph(get, fp_in, fp_out):
    g = Graph()
    for line in fp:
        g.add_dataset(json.loads(line))
    for left, right in g.similarly_indexed_datasets():
        fp.write((left, right))

#   with open('blizzard.p', 'wb') as fp:
#       pickle.dump(g, fp)

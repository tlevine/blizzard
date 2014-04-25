import logging
import functools
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from threading import Thread

from pluplusch import pluplusch

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

    command = u.parser().parse_args().command
    if command == 'index':
        index(sys.stdout)
    elif command == 'graph':
        graph(sys.stdin, sys.stdout)

def index(fp_out):
    state = {'downloading': True, 'futures': {}}
    with ProcessPoolExecutor(4) as e:
        def divine():
            for dataset in pluplusch(catalogs = dl.catalogs, cache_dir = '.blizzard', proxies = proxies()):
                if not u.ignore(dataset):
                    state['futures'][(dataset['catalog'], dataset['datasetid'])] = e.submit(meta.snowflake, dataset)
            state['downloading'] = False
            
        Thread(None, target = divine, name = 'download datasets', args = ()).start()
        while state['downloading'] or state['futures'] != {}:
            for key, future in list(state['futures'].items()):
                if future.done():
                    dataset = future.result()
                    fp_out.write(json.dumps(dataset) + '\n')
                    del(state['futures'][key])
                    logger.debug('In line for snowflaking: %s' % state['futures'].keys())

def graph(fp_in, fp_out):
    g = Graph()
    for line in fp_in:
        g.add_dataset(json.loads(line))
    for left, right in g.similarly_indexed_datasets():
        fp_out.write(u.dataset_url(*left) + '  <-->  ' + u.dataset_url(*right) + '\n')

def proxies():
    if 'http_proxy' in os.environ:
        logger.info('Proxy: %s' % os.environ['http_proxy'])
        the_proxies = {'http_proxy': os.environ['http_proxy']}
    else:
        logger.info('Proxy: No proxy')
        the_proxies = {}
    logger.debug('Using proxies %s' % the_proxies)
    return the_proxies

import os
import argparse
from logging import getLogger

import requests
from pickle_warehouse import Warehouse
from picklecache import downloader as _downloader

logger = getLogger('blizzard')

def _get(url:str):
    if 'http_proxy' in os.environ:
        logger.info('Proxy: %s' % os.environ['http_proxy'])
        proxies = {'http_proxy': os.environ['http_proxy']}
    else:
        logger.info('Proxy: No proxy')
        proxies = {}
    logger.debug('%s: Downloading with proxies %s' % (url,proxies))
    return requests.get(url, proxies = proxies)

downloader = lambda datadir: _downloader(_get, Warehouse(datadir))

def ignore(dataset):
    if 'geo_shape' in set(f['type'] for f in dataset['fields']):
        args = (dataset['catalog'], dataset['datasetid'])
        logger.debug('%s, %s: csv parser can\'t handle geo data')
        return True
    elif not dataset['download'].ok:
        args = (dataset['catalog'], dataset['datasetid'], dataset['download'].status_code)
        logger.debug('%s, %s: Skipping status code %d' % args)
        return True
    elif dataset['download'].text.strip() == '':
        args = (dataset['catalog'], dataset['datasetid'])
        logger.debug('%s, %s: empty file')
        return True
    else:
        return False

def parser():
    p = argparse.ArgumentParser()
    p.add_argument('command', choices = ['index', 'graph'])
    return p

def dataset_url(catalog, datasetid):
    return '%s/explore/dataset/%s' % (catalog, datasetid)

def dataset_download_url(catalog, datasetid):
    return '%s/explore/dataset/%s/download/?format=csv' % (catalog, datasetid)

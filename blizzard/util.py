from logging import getLogger

import requests
import pickle_warehouse

logger = getLogger('blizzard')

def _get(datadir:str, url:str):
    warehouse = pickle_warehouse.Warehouse(datadir)
    if url in warehouse:
        logger.debug('%s: Loading from cache' % url)
        response = warehouse[url]
    else:
        logger.debug('%s: Downloading' % url)
        response = requests.get(url)
        warehouse[url] = response
    return response

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

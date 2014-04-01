import os
from logging import getLogger

import requests
import pickle_warehouse

logger = getLogger('blizzard')

if 'http_proxy' in os.environ:
    logger.info('Proxy: %s' % os.environ['http_proxy'])
    proxies = {'http_proxy': os.environ['http_proxy']}
else:
    logger.info('Proxy: No proxy')
    proxies = {}

def _get(datadir:str, url:str):
    warehouse = pickle_warehouse.Warehouse(datadir)
    if url in warehouse:
        logger.debug('%s: Loading from cache' % url)
        response = warehouse[url]

        # Clear bad files
        if not response.ok:
            logger.error('%s: %d in cached response' % (url, response))
            del(warehouse[url])
            return _get(datadir, url)
    else:
        logger.debug('%s: Downloading' % url)
        response = requests.get(url, proxies = proxies)

        # Stop on bad files
        if not response.ok:
            logger.error('%s: %d in new response' % (url, response))
            raise ValueError('Bad HTTP response')

        warehouse[url] = response

    return response

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

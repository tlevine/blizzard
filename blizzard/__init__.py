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


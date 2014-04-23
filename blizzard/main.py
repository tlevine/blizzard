import logging
import functools

from blizzard.nxgraph import Graph

def main():
    datadir = os.path.expanduser('~/dadawarehouse.thomaslevine.com/big/opendatasoft')
    n_workers = 30

    # Logging
    fp_log = logging.FileHandler('blizzard.log', 'w')
    logger.setLevel(logging.DEBUG)
    fp_log.setLevel(logging.DEBUG)
    logger.addHandler(fp_log)
    logger.debug('Starting a new run\n==============================================')

    get = functools.partial(_get, datadir)
    for catalog in catalogs:
        def f(dataset):
            dataset['download'] = download(get, catalog, dataset['datasetid'])
            return dataset
        for future in jumble(f, datasets(get, catalog), n_workers):
            yield future.result()

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    return {'nrow': nrow}

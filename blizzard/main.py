def get(url, **kwargs):
    from get import get as _get
    return _get(url, cachedir = 'data', **kwargs)

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

def main():
    logger = logging.getLogger('featured-spreadsheets')
    logger.setLevel(logging.DEBUG)

    h1 = logging.FileHandler("featured-spreadsheets.log","w")
    h1.setLevel(logging.DEBUG)
    logger.addHandler(h1)

    h2 = logging.StreamHandler()
    h2.setLevel(logging.ERROR)
    logger.addHandler(h2)

    g = Graph()
    for dataset in :
        g.add_dataset(dataset)
    with open('graph.p', 'wb') as fp:
        pickle.dump(g, fp)

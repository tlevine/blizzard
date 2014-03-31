from concurrent.futures import ThreadPoolExecutor
import json

catalogs = [
    'http://data.iledefrance.fr',
    'http://opendata.paris.fr.opendatasoft.com',
    'http://tourisme04.opendatasoft.com',
    'http://tourisme62.opendatasoft.com',
    'http://grandnancy.opendatasoft.com',
    'http://bistrotdepays.opendatasoft.com',
    'http://scisf.opendatasoft.com',
    'http://pod.opendatasoft.com',
    'http://dataratp.opendatasoft.com',
    'http://public.opendatasoft.com',
    'http://ressources.data.sncf.com',
]

def datasets(get, catalog):
    # Search an OpenDataSoft portal, and add things.
    # I chose OpenDataSoft because they care a lot about metadata.
    raw = get(catalog + '/api/datasets/1.0/search?rows=1000000', load = True)
    return json.loads(raw.decode('utf-8'))['datasets']

def download(get, catalog, datasetid):
    url = '%s/explore/dataset/%s/download?format=csv' % args
    get(url, load = False)

def _run(get, catalog):
    n = 30
    datasetids = (d['datasetid'] for d in datasets(get,catalog))
    with ThreadPoolExecutor(n) as e:
        yield from e.map(partial(get,catalog), datasetids)

def run(get):
    with ThreadPoolExecutor(len(catalogs)) as e:
        yield from e.map(partial(_run, get),catalogs)

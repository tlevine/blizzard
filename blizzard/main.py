import pickle_warehouse

from blizzard.graph import Graph
import blizzard.download as download

def get(load:bool, url:str):
    warehouse = pickle_warehouse.Warehouse(os.path.expanduser('~/dadawarehouse.thomaslevine.com/opendatasoft'))
    if url in warehouse:
        response = warehouse[url]
    else:
        response = requests.get(url)
        warehouse[url] = response
    return response

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

def main():
    g = Graph()
    for dataset in download.all(get):
        response = download.download(get, dataset['catalog'], dataset['datasetid'])
        if not ignore(dataset):
            with StringIO(response.text) as fp:
                dataset['unique_indices'] = unique_indices(fp, url)
            g.add_dataset(dataset)
    with open('graph.p', 'wb') as fp:
        pickle.dump(g, fp)


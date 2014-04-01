import requests
import pickle_warehouse

def _get(datadir:str, load:bool, url:str):
    warehouse = pickle_warehouse.Warehouse(datadir)
    if url in warehouse:
        response = warehouse[url]
    else:
        response = requests.get(url)
        warehouse[url] = response
    return response

def ignore(dataset):
    return 'geo_shape' in set(f['type'] for f in dataset['fields'])

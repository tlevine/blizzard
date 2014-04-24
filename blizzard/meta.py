import csv, json
from io import StringIO

from more_itertools import ilen
from special_snowflake import fromcsv

def snowflake(fp, dataset):
    dataset.update(individual_metadata(dataset['download'].text))
    del(dataset['download'])
    fp(json.dumps(dataset) + '\n')

def individual_metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    with StringIO(dataset_text) as fp:
        unique_keys = list(sorted(fromcsv(fp, delimiter = ';')))
    return {'nrow': nrow, 'unique_keys': unique_keys}

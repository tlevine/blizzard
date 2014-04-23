from logging import getLogger
logger = getLogger('blizzard')

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    return {'nrow': nrow}

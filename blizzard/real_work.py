from logging import getLogger
logger = getLogger('blizzard')

def snow(dataset):
    n_columns = 3
    dataset = dict(dataset)
    logger.debug('%s, %s: Got dataset' % (dataset['catalog'], dataset['datasetid']))
    if ignore(dataset):
        logger.debug('%s, %s: Skipping geographic data' % (dataset['catalog'], dataset['datasetid']))
        dataset['unique_indices'] = set()
    elif not dataset['download'].ok:
        args = (dataset['catalog'], dataset['datasetid'], dataset['download'].status_code)
        logger.debug('%s, %s: Skipping status code %d' % args)
        dataset['unique_indices'] = set()
    else:
        logger.debug('%s, %s: Snowflaking' % (dataset['catalog'], dataset['datasetid']))
        with StringIO(dataset['download'].text) as fp:
            try:
                dataset['unique_indices'] = fromcsv(fp, delimiter = ';', n_columns = n_columns)
            except:
                tb = formatter.formatException(sys.exc_info())
                logger.error('%s, %s:\n%s\n\n' % (dataset['catalog'], dataset['datasetid'], tb))
                dataset['unique_indices'] = set()

        logger.debug('%s, %s: Found indices %s' % (dataset['catalog'], dataset['datasetid'], dataset['unique_indices']))
    return dataset

def metadata(dataset_text):
    with StringIO(dataset_text) as fp:
        r = csv.DictReader(fp, delimiter = ';')
        nrow = ilen(r)
    return {'nrow': nrow}

from collections import namedtuple

import nose.tools as n

import blizzard.real_work as rw

Response = namedtuple('Response', ['ok', 'status_code', 'text'])
with open(os.path.join('blizzard','test','fixtures','dataset.p'),'rb') as fp:
    dataset = fp.read()

    if ignore(dataset):
        logger.debug('%s, %s: Skipping geographic data' % (dataset['catalog'], dataset['datasetid']))
        dataset['unique_indices'] = set()
    elif not dataset['download'].ok:
        args = (dataset['catalog'], dataset['datasetid'], dataset['download'].status_code)
        logger.debug('%s, %s: Skipping status code %d' % args)
        dataset['unique_indices'] = set()
    else:

csv = '''a,b,c
8,4,3
8,4,2
8,4,8
'''

def test_unique_indices():
    dataset = {
        'catalog': 'foo',
        'datasetid': 'bar',
        'download': Response(True, 200, csv)
    }
    n.assert_set_equal(rw.unique_indices(dataset), {'c'})
    dataset = {
        'catalog': 'foo',
        'datasetid': 'bar',
        'download': Response(False, 404, csv)
    }
    n.assert_set_equal(rw.unique_indices(dataset), {})

def test_blizzard():
    observed = rw.blizzard(dataset)
    expected = 

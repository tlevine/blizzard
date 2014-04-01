from collections import namedtuple
import json

import nose.tools as n

import blizzard.download

Response = namedtuple('Response', ['text'])

def test_datasets():
    def get(url):
        return Response(text = '{"datasets":[{"foo":8}]}')
    catalog = 'stnoheustahoe'

    observed = blizzard.download.datasets(get, catalog)
    expected = [{'foo':8,'catalog':catalog}]
    n.assert_list_equal(observed, expected)

def test_run():
    datasets = [{'datasetid':3}]
    def get(url):
        return Response(text = json.dumps({'datasets':datasets}))
    observed = list(blizzard.download._run(get, 'aoeu'))
    expected = datasets
    n.assert_list_equal(observed, expected)

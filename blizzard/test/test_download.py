from collections import namedtuple

import nose.tools as n

import blizzard.download

def test_datasets():
    def get(url):
        Response = namedtuple('Response', ['text'])
        return Response(text = '{"datasets":[{"foo":8}]}')
    catalog = 'stnoheustahoe'

    observed = blizzard.download.datasets(get, catalog)
    expected = [{'foo':8,'catalog':catalog}]
    n.assert_list_equal(observed, expected)

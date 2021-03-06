from collections import namedtuple
import json

import nose.tools as n

import blizzard.download

Response = namedtuple('Response', ['text','ok'])

def test_datasets():
    def get(url):
        return Response(text = '{"datasets":[{"foo":8}]}', ok = True)
    catalog = 'stnoheustahoe'

    observed = blizzard.download.datasets(get, catalog)
    expected = [{'foo':8,'catalog':catalog}]
    n.assert_list_equal(observed, expected)

def test_run():
    r = Response(text = json.dumps({'datasets':[{'datasetid':3}]}), ok = True)
    def get(url):
        return r
    observed = list(blizzard.download._run(get, 'aoeu'))
    expected = [{'datasetid':3,'catalog':'aoeu','download':r}]
    n.assert_list_equal(observed, expected)

def test_all():
    r = Response(text = json.dumps({'datasets':[{'datasetid':3,'catalog':'hh'}]}), ok = True)
    def get(url):
        return r
    observed = list(blizzard.download.all(get))
    n.assert_equal(len(observed), len(blizzard.download.catalogs))
    n.assert_set_equal(set(map(type,observed)),{dict})

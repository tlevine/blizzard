import nose.tools as n

import blizzard.download

def test_datasets():
    def get(url):
        return '{"datasets":[{"foo":8}]}'
    catalog = 'stnoheustahoe'

    observed = blizzard.download.datasets(get, catalog)
    expected = [{'foo':8,'catalog':catalog}]
    n.assert_list_equal(observed, expected)

import pickle,os

import nose.tools as n

from blizzard import _snow

@n.nottest
def test_snow():
    'We should somehow find out about the status code issue.'
    with open(os.path.join('blizzard','test','fixture','budget-credits_de_paiement.p'),'rb') as fp:
        dataset = {'datasetid':8,'catalog':'foo','fields':[],'download':pickle.load(fp)}
    observed = _snow(dataset)['unique_indices']
    expected = set()
    n.assert_set_equal(observed, expected)

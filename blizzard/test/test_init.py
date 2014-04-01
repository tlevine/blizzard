import pickle,os

import nose.tools as n

from blizzard import _snow

def test_snow():
    with open(os.path.join('blizzard','test','fixture','budget-credits_de_paiement.p'),'rb') as fp:
        dataset = {'datasetid':8,'catalog':'foo','download':pickle.load(fp)}
    observed = _snow(dataset)['unique_indices']
    expected = set()
    n.assert_set_equal(observed, expected)

import os, pickle

import nose.tools as n

from blizzard.util import ignore

def test_ignore():
    path = os.path.join('blizzard','test','fixture','lieux-de-tournage-de-films-long-metrage-paris.p')
    with open(path, 'rb') as fp:
        dataset = {
            'fields': [],
            'download': pickle.load(fp),
        }
    n.assert_true(ignore(dataset))

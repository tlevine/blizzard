from itertools import combinations
import json, pickle

import networkx as nx

class Graph(nx.Graph):
    def add_dataset(self, dataset):
        dataset_id = (dataset['catalog'], dataset['datasetid'])
       #dataset_url = '%s/explore/dataset/%s' % (dataset['catalog'], dataset['datasetid'])
        self.add_node(dataset_id, kind = 'dataset')
        for unique_index in dataset['unique_keys']:
            i = tuple(sorted(unique_index))
            self.add_node(i, kind = 'index')
            self.add_edge(i, dataset_id)

    def similarly_indexed_datasets(self):
        'Returns an iterable of (dataset node, dataset node)'
        indices = (index for index,data in self.nodes(data = True) if data['kind'] == 'index')
        seen = set()
        for index in indices:
            for datasets in combinations(self.neighbors(index),2):
                a, b = tuple(sorted(datasets))
                if not ((a, b) in seen or (b, a) in seen):
                    yield a, b
                    seen.add((a, b))

def dataset_url(node):
    name, data = node
    if data['kind'] == 'dataset':
        return '%s/explore/dataset/%s' % name

def main():
    g = Graph()

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--jsonlines', metavar = 'JSONLINES FILE',  default = 'blizzard.jsonlines')
    parser.add_argument('-g', '--graph', metavar = 'GRAPH FILE',  default = 'blizzard.p')
    with open(parser.parse_args().jsonlines) as fp:
        for line in fp:
            g.add_dataset(json.loads(line))
    for left, right in g.similarly_indexed_datasets():
        print(left, right)

#   with open('blizzard.p', 'wb') as fp:
#       pickle.dump(g, fp)

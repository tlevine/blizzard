from itertools import combinations

import networkx as nx

class Graph(nx.Graph):
    def add_dataset(self, dataset):
        dataset_id = (dataset['catalog'], dataset['datasetid'])
        self.add_node(dataset_id, kind = 'dataset', text = dataset['download'].text)
        for unique_index in dataset['unique_indices']:
            i = tuple(sorted(unique_index))
            self.add_node(i, kind = 'index')
            self.add_edge(i, dataset_id)

    def refactor(self, overlap):
        indices = (index for index,data in self.nodes(data = True) if data['kind'] == 'index')
        for index in indices:
            for datasets in combinations(self.neighbors(index),2):
                a, b = tuple(sorted(_datasets))
                if b not in self[a]: # if edge doesn't exist
                   texts = nx.get_node_attributes(self, 'text')
                   node_attrs, edge_attrs = overlap(texts['a'], texts['b'])
                   
                   self.add_edge(a, b)
                   nx.set_node_attributes(self, 'nrow', node_attrs)
                   nx.set_edge_attributes(self, 'nrow', edge_attrs)

import networkx as nx

class Graph(nx.Graph):
    def add_dataset(self, dataset):
        dataset_id = dataset['datasetid']
        for unique_index in dataset['unique_indices']:
            self.add_edge(tuple(sorted(unique_index)),dataset_id)

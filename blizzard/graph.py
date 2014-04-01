import networkx as nx

class Graph(nx.Graph):
    def add_dataset(self, dataset):
        dataset_id = (dataset['catalog'], dataset['datasetid'])
        for unique_index in dataset['unique_indices']:
            i = tuple(sorted(unique_index))
            self.add_node(i, kind = 'index')
            self.add_node(dataset_id, kind = 'dataset', text = dataset['download'].text)
            self.add_edge(i, dataset_id)
            for neighbor_datasetid in self.neighbors(i, data = True):
                j = tuple(sorted(neighbor_datasetid, datasetid))
                if neighbor_datasetid != datasetid and j not in self.edges_iter():
                   texts = nx.get_node_attributes(self, 'text')
                   attr = overlap(texts['neighbor_datasetid'], texts['datasetid'])
                   self.add_edge(neighbor_datasetid, datasetid, attr)

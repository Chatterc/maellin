import unittest
from cne.pipelines.graph import DAG
import networkx as nx
from uuid import uuid4


class Task():
    task_id = 0
    """Dummy Container"""

    def __init__(self, desc='test'):
        Task.task_id += 1
        self.tid = Task.task_id
        self.desc = desc


class TestDAG(unittest.TestCase):

    def setUp(self):
        self.dag = DAG()

    def test_merge(self):
        G = nx.MultiDiGraph({1: {2}})
        H = nx.MultiDiGraph({1: {3}})
        DAG = self.dag.merge(G, H)
        edges = list(DAG.edges())
        self.assertEqual([(1, 2), (1, 3)], edges)

    def test_add_edge_to_dag(self):
        self.dag.add_edge_to_dag(1, 1, 2, str(uuid4()))
        edges = list(self.dag.dag.edges(1))
        self.assertEqual(edges, [(1, 2)])

    def test_is_dag(self):
        G = nx.DiGraph([(1, 2), (1, 3), (2, 4), (3, 4)])
        self.dag.dag.add_nodes_from(G)
        self.assertTrue(self.dag.is_dag())

    def test_topological_sort(self):
        G = nx.DiGraph([(1, 2), (1, 3), (2, 4), (3, 4)])
        self.dag.dag.add_nodes_from(G)
        sort = [id for id in self.dag.topological_sort()]
        self.assertEqual(sort, [1, 2, 3, 4])

    def test_get_all_nodes(self):
        G = nx.DiGraph([(1, 2), (1, 3), (2, 4), (3, 4)])
        self.dag.dag.add_nodes_from(G)
        nodes = self.dag.get_all_nodes()
        self.assertEqual([key for key in nodes.keys()], [1, 2, 3, 4])

    def test_get_all_edges(self):
        self.dag.dag.add_edges_from([(1, 2), (1, 3), (2, 4), (3, 4)])
        edges = [k for k in self.dag.get_all_edges().keys()]

        self.assertEqual(edges, [(1, 2, 0), (1, 3, 0), (2, 4, 0), (3, 4, 0)])

    def test_get_predecessors(self):
        self.dag.dag.add_edges_from([(1, 2), (1, 3), (2, 4), (3, 4)])
        pred = self.dag.get_predecessors(4)
        self.assertEqual(pred, [2, 3])

    def test_get_successors(self):
        self.dag.dag.add_edges_from([(1, 2), (1, 3), (2, 4), (3, 4)])
        succ = self.dag.get_successors(1)
        self.assertEqual(succ, [2, 3])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()

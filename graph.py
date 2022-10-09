
from typing import Dict, List, Type, TypeVar
from uuid import uuid4

from cne.pipelines.utils import CircularDependencyError, MissingDependencyError
from networkx import (
    MultiDiGraph,
    compose,
    is_directed_acyclic_graph,
    is_weakly_connected,
    number_of_nodes,
    is_empty,
    topological_sort
)


Activity = TypeVar('Activity')


class DAG:

    def __init__(self) -> None:
        """Base Directed Acyclic Graph Class with parallel edge support
        """
        self.dag = MultiDiGraph()

    def _validate_dag(self) -> None:
        """Validates Pipeline is constructed properly

        Raises:
            CircularDependencyError: Error if DAG contains cycles
            MissingDependencyError: Error raised if DAG contains disconnected nodes
        """

        # Validate DAG does not have cycles
        if not self.is_dag():
            raise CircularDependencyError("DAG Contains Cycles, Check Steps for Circular Dependencies")

        # Validate DAG does not have weakly connected nodes
        if not self.is_weakly_connected():
            raise MissingDependencyError("DAG Contains Weakly Connected Nodes, Check Steps for Missing Dependencies")

    def merge(self, G: MultiDiGraph, H: MultiDiGraph) -> MultiDiGraph:
        """Returns a new graph of G composed with H. Composition is the \
        simple union of the node sets and edge sets. The node sets  \
        of G and H do not need to be disjoint. Note: Edges in G that have \
        the same edge key as edges in H will be overwritten with edges from H

        Args:
            G (MultiDiGraph): First MultiDiGraph Instance
            H (MultiDiGraph): Second MultiDigraph Instance

        Returns:
            DAG (MultiDiGraph): A Direct Acyclic Graph that support parallel edges
        """
        return compose(G, H)

    def add_node_to_dag(self,
                        activity: Type[Activity] = None,
                        properties: Dict = None) -> None:
        """Adds a new Node to the DAG with attributes

        Args:
            activity (Type[Activity], optional): Activity Instance. Defaults to None.
            properties (Dict, optional): User Properties. Defaults to None.
        """
        # if the node already exists
        if activity.task.tid in list(self.dag.nodes):
            existing = dict(self.dag.nodes(data='activities')).get(activity.task.tid, None)
            if existing is not None:
                updates = existing.update({activity.id: activity})
                return
            else:
                updates = {activity.id: activity}
        else:
            updates = {activity.id: activity}
        # Add a new node to the DAG
        self.dag.add_nodes_from([
            (
                activity.task.tid, {
                    "id": activity.task.tid,
                    "activities": updates,
                    "properties": properties
                }
            )
        ])

    def add_edge_to_dag(
            self,
            pid: int,
            tid_from: int,
            tid_to: int,
            activity_id: uuid4) -> None:
        """Adds an edge between two nodes to the DAG

        Args:
            tid_from (int): The dependency Task unique ID "tid"
            tid_to (int): The Task unique ID "tid"
            activity_id (uuid): Activity Id used to define the edge
        """
        # Add the edge to the DAG
        self.dag.add_edges_from([
            (
                tid_from, tid_to, activity_id, {
                    "pid": pid,
                    "tid_from": tid_from,
                    "tid_to": tid_to,

                }
            )
        ])

    def is_dag(self) -> bool:
        """Validates all edges are directed and no cycles exist within the DAG"""
        # Checks that graph is a DAG
        return is_directed_acyclic_graph(G=self.dag)

    def is_weakly_connected(self) -> bool:
        """Validates all edges and nodes are"""
        return is_weakly_connected(G=self.dag)

    def is_empty(self) -> bool:
        """Returns True if DAG is empty"""
        if number_of_nodes(self.dag) < 1:
            if is_empty(self.dag):
                return True
        return False

    def topological_sort(self) -> List:
        """Sorts nodes in topological order for queuing tasks

        Returns:
            List: nodes containing Tasks in topological sort order
        """
        # Topical sort of Tasks into a list
        return list(topological_sort(G=self.dag))

    def get_all_nodes(self) -> dict:
        """Get all Nodes from the DAG

        Returns:
            nodes (dict): dictionary of nodes contained in the DAG
        """
        # Returns all nodes + node attributes as a dict of dicts
        nodes = dict(self.dag.nodes(data=True))
        return nodes

    def get_all_edges(self) -> dict:
        """Get all edges from the DAG

        Returns:
            edges (dict): dictionary of edges contained in the DAG
        """
        # Returns all nodes + node attributes as a dict of dicts
        edges = dict(self.dag.edges(keys=True))
        return edges

    def get_all_attributes(self, name: str = None) -> dict:
        """Gets all matching attributes found in a Graph

        Args:
            name (str, optional): Name of the attribute. Defaults to None.

        Returns:
            dict: a dict of dicts containing matching attributes of a graph
        """
        attr = dict(self.dag.nodes(data=name, default=None)).values()
        return attr

    def get_predecessors(self, n) -> List:
        """Returns a list of all predecessors of a node, \
        such that there exists a directed edge from m to n

        Args:
            n (node): A node in the DAG

        Returns:
            List: list of predecessors
        """
        return list(self.dag.predecessors(n))

    def get_successors(self, n) -> List:
        """Returns a list of all successors of a node, \
        such that there exists a directed edge from m to n

        Args:
            n (node): A node in the DAG

        Returns:
            List: list of predecessors
        """
        return list(self.dag.successors(n))

    def repair_attributes(self, G: MultiDiGraph, H: MultiDiGraph, attr: str) -> None:
        """Preserved node attributes that may be overwritten when using merge.
        Note: this method only works if the attribute being preserved is a dictionary

        Args:
            G (MultiDiGraph): left graph
            H (MultiDiGraph): right graph
            attr (str): name of attribute
        """
        for node in G.nodes():
            if node in H:
                if self.dag.nodes[node].get(attr, None) is not None:
                    attr_G = G.nodes[node].get(attr)
                    if attr_G is not None:
                        attr_H = H.nodes[node].get(attr)
                        if attr_G == attr_H:
                            continue
                        else:
                            attr_G.update(attr_H)
                            self.dag.nodes[node][attr] = attr_G

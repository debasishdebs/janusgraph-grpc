import unittest

from janusgraph.management.janusgraph_management import JanusGraphManagement
from janusgraph.management.element.edge_label import EdgeLabel


class EdgeLabelManagementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.HOST = "localhost"
        self.PORT = 10182
        self.GRAPH = "graph_inmemory"
        pass

    def test_create_basic_edge_label(self):
        """Creates a basic edge label without any extra properties

        Returns:

        """

        default_label = "test1"
        default_multiplicity = "Multi"
        default_directed = True
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_undirecred_edge_label(self):
        default_label = "test2"
        default_multiplicity = "Multi"
        default_directed = False
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).undirected().make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_directed_simple_multiplicity_edge_label(self):
        default_label = "test3"
        default_multiplicity = "Simple"
        default_directed = True
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("Simple").make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_undirected_simple_multiplicity_edge_label(self):
        default_label = "test4"
        default_multiplicity = "Simple"
        default_directed = False
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("Simple").undirected().make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_directed_one2many_multiplicity_edge_label(self):
        default_label = "test5"
        default_multiplicity = "One2Many"
        default_directed = True
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("One2Many").make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_undirected_one2many_multiplicity_edge_label(self):
        default_label = "test6"
        default_multiplicity = "One2Many"
        default_directed = False
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        maker = mgmt.makeEdgelabel(default_label).multiplicity("One2Many").undirected()

        self.assertRaises(Exception, maker.make, None)

        mgmt.close()

    def test_create_directed_many2one_multiplicity_edge_label(self):
        default_label = "test7"
        default_multiplicity = "Many2One"
        default_directed = True
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("Many2One").make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_undirected_many2one_multiplicity_edge_label(self):
        default_label = "test8"
        default_multiplicity = "Many2One"
        default_directed = False
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("Many2One").undirected().make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_directed_one2one_multiplicity_edge_label(self):
        default_label = "test9"
        default_multiplicity = "One2One"
        default_directed = True
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        edges = mgmt.makeEdgelabel(default_label).multiplicity("One2One").make()

        self.assertEqual(type(edges), list)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(type(edge), EdgeLabel)
        self.assertEqual(edge.getLabel(), default_label)
        self.assertEqual(edge.getMultiplicity(), default_multiplicity)
        self.assertEqual(edge.getDirected(), default_directed)
        self.assertEqual(edge.getDirection(), default_direction)

        print(edge)

        mgmt.close()

    def test_create_undirected_one2one_multiplicity_edge_label(self):
        default_label = "test10"
        default_multiplicity = "One2One"
        default_directed = False
        default_direction = "BOTH"

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        maker = mgmt.makeEdgelabel(default_label).multiplicity("One2One").undirected()

        self.assertRaises(Exception, maker.make, None)

        mgmt.close()

    def test_get_edges(self):
        to_check = {
            "test1": {
                "multiplicity": "Multi",
                "directed": True,
                "direction": "BOTH"
            },
            "test2": {
                "multiplicity": "Multi",
                "directed": False,
                "direction": "BOTH"
            },
            "test3": {
                "multiplicity": "Simple",
                "directed": True,
                "direction": "BOTH"
            },
            "test4": {
                "multiplicity": "Simple",
                "directed": False,
                "direction": "BOTH"
            },
            "test5": {
                "multiplicity": "One2Many",
                "directed": True,
                "direction": "BOTH"
            },
            "test7": {
                "multiplicity": "Many2One",
                "directed": True,
                "direction": "BOTH"
            },
            "test8": {
                "multiplicity": "Many2One",
                "directed": False,
                "direction": "BOTH"
            },
            "test9": {
                "multiplicity": "One2One",
                "directed": True,
                "direction": "BOTH"
            }
        }

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)

        for k, v in to_check.items():
            label = k
            multiplicity = v["multiplicity"]
            is_directed = v["directed"]
            direction = v["direction"]

            edges = mgmt.getEdgeLabel(label)

            self.assertEqual(type(edges), list)
            self.assertEqual(len(edges), 1)
            edge = edges[0]
            self.assertEqual(type(edge), EdgeLabel)

            self.assertEqual(edge.getLabel(), label)
            self.assertEqual(edge.getMultiplicity(), multiplicity)
            self.assertEqual(edge.getDirected(), is_directed)
            self.assertEqual(edge.getDirection(), direction)

        mgmt.close()


if __name__ == '__main__':
    unittest.main()

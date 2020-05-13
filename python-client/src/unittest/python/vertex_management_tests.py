import unittest

from janusgraph.management.janusgraph_management import JanusGraphManagement
from janusgraph.management.element.vertex_label import VertexLabel


class VertexLabelManagementTest(unittest.TestCase):

    def setUp(self) -> None:
        self.HOST = "localhost"
        self.PORT = 10182
        self.GRAPH = "graph_inmemory"

    def test_create_basic_vertex_label(self):
        """Creates a basic vertex label without any extra properties like static, partition etc

        Returns:

        """

        default_label = "test1"
        default_static = False
        default_partition = False

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        vertex = mgmt.makeVertexLabel(default_label).make()

        self.assertEqual(type(vertex), list)
        self.assertEqual(len(vertex), 1)
        vertex = vertex[0]
        self.assertEqual(type(vertex), VertexLabel)
        self.assertEqual(vertex.getLabel(), default_label)
        self.assertEqual(vertex.getStatic(), default_static)
        self.assertEqual(vertex.getPartitioned(), default_partition)

        print(vertex)

        mgmt.close()

    def test_create_static_vertex_label(self):
        default_label = "test2"
        default_partition = False

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        vertex = mgmt.makeVertexLabel(default_label).setStatic().make()

        self.assertEqual(type(vertex), list)
        self.assertEqual(len(vertex), 1)
        vertex = vertex[0]
        self.assertEqual(type(vertex), VertexLabel)
        self.assertEqual(vertex.getLabel(), default_label)
        self.assertEqual(vertex.getStatic(), True)
        self.assertEqual(vertex.getPartitioned(), default_partition)

        print(vertex)

        mgmt.close()

    def test_create_partitioned_vertex_label(self):
        default_label = "test3"
        default_static = False

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        vertex = mgmt.makeVertexLabel(default_label).partition().make()

        self.assertEqual(type(vertex), list)
        self.assertEqual(len(vertex), 1)
        vertex = vertex[0]
        self.assertEqual(type(vertex), VertexLabel)
        self.assertEqual(vertex.getLabel(), default_label)
        self.assertEqual(vertex.getStatic(), default_static)
        self.assertEqual(vertex.getPartitioned(), True)

        print(vertex)

        mgmt.close()

    def test_create_static_partition_vertex_label_exception(self):
        default_label = "test4"
        default_static = True
        default_partition = True

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        maker = mgmt.makeVertexLabel(default_label).setStatic().partition()

        self.assertRaises(Exception, maker.make, None)

        mgmt.close()

    def test_get_vertex(self):
        to_check = {
            "test1": {
                "static": False,
                "partitioned": False
            },
            "test2": {
                "static": True,
                "partitioned": False
            },
            "test3": {
                "static": False,
                "partitioned": True
            }
        }

        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)

        for k, v in to_check.items():
            label = k
            is_static = v["static"]
            is_partitioned = v["partitioned"]

            vertex = mgmt.getVertexLabel(label)

            self.assertEqual(type(vertex), list)
            self.assertEqual(len(vertex), 1)
            vertex = vertex[0]
            self.assertEqual(type(vertex), VertexLabel)

            print(vertex)

            self.assertEqual(vertex.getLabel(), label)
            self.assertEqual(vertex.getStatic(), is_static)
            self.assertEqual(vertex.getPartitioned(), is_partitioned)

    def test_get_all_vertex_labels(self):
        mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)
        vertices = mgmt.getVertexLabels()

        for v in vertices:
            print(v)

        mgmt.close()


if __name__ == '__main__':
    unittest.main()

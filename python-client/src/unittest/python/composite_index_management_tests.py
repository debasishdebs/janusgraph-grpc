import unittest
import time
from janusgraph.management.janusgraph_management import JanusGraphManagement


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.HOST = "localhost"
        self.PORT = 10182
        self.GRAPH = "graph_inmemory"

        self.mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)

    def test_create_basic_vertex_composite_index(self):
        index_name = "index1"
        property_key = "index_prop1"

        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex").addKey(property_key).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), "ALL")
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")
        print(len(index.getProperties()))
        print(len(index_prop))
        for p in index.getProperties():
            print(p)
        print(50*"-_")
        for p in index_prop:
            print(p)
        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

    def test_create_basic_vertex_composite_index_and_enable_it(self):
        index_name = "index2"
        property_key = "index_prop2"

        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex").addKey(property_key).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), "ALL")
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_basic_edge_composite_index(self):
        # Fails. Should pass once we deploy the new edge based management for server
        index_name = "index3"
        property_key = "index_prop3"

        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Edge").addKey(property_key).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), "ALL")
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "EdgeLabel")

        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

    def test_create_basic_edge_composite_index_and_enable_it(self):
        # Fails. Should pass once we deploy the new edge based management for server
        index_name = "index4"
        property_key = "index_prop4"

        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Edge").addKey(property_key).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), "ALL")
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "EdgeLabel")

        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_constrained_vertex_composite_index(self):
        index_name = "index5"
        property_key = "index_prop5"
        constrained_label = "index_vertex1"

        label = self.mgmt.makeVertexLabel(constrained_label).make()
        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex").addKey(property_key).indexOnly(constrained_label).build()

        self.assertTrue(len(index) == len(label) == 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), label[0].getLabel())
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_constrained_unique_vertex_composite_index(self):
        index_name = "index6"
        property_key = "index_prop6"
        constrained_label = "index_vertex2"

        label = self.mgmt.makeVertexLabel(constrained_label).make()
        index_prop = self.mgmt.makePropertyKey(property_key).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex").addKey(property_key).\
            indexOnly(constrained_label).unique().build()

        print("Index created is ")
        print(index)

        self.assertTrue(len(index) == len(label) == 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), True)
        self.assertEqual(index.getConstrainedLabel(), label[0].getLabel())
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len(index_prop))
        self.assertTrue(all(index.getProperties()[i].getName() == index_prop[i].getName()
                            for i in range(len(index_prop))))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_basic_vertex_composite_index_on_two_properties(self):
        index_name = "index7"
        property_key_1 = "index_prop7"
        property_key_2 = "index_prop8"

        index_prop_1 = self.mgmt.makePropertyKey(property_key_1).make()
        index_prop_2 = self.mgmt.makePropertyKey(property_key_2).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex")\
            .addKey(property_key_1).addKey(property_key_2).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), "ALL")
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len([index_prop_1, index_prop_2]))

        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_1[0].getName() for i in range(2)))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_2[0].getName() for i in range(2)))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_vertex_composite_index_on_two_properties_constrained_by_label(self):
        index_name = "index8"
        property_key_1 = "index_prop9"
        property_key_2 = "index_prop10"
        constrained_label = "index_vertex3"

        label = self.mgmt.makeVertexLabel(constrained_label).make()[0]
        index_prop_1 = self.mgmt.makePropertyKey(property_key_1).make()
        index_prop_2 = self.mgmt.makePropertyKey(property_key_2).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex") \
            .addKey(property_key_1).addKey(property_key_2).indexOnly(constrained_label).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), False)
        self.assertEqual(index.getConstrainedLabel(), label.getLabel())
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len([index_prop_1, index_prop_2]))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_1[0].getName() for i in range(2)))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_2[0].getName() for i in range(2)))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_unique_vertex_composite_index_on_two_properties_constrained_by_label(self):
        # Should Fail
        index_name = "index9"
        property_key_1 = "index_prop11"
        property_key_2 = "index_prop12"
        constrained_label = "index_vertex4"

        label = self.mgmt.makeVertexLabel(constrained_label).make()[0]
        index_prop_1 = self.mgmt.makePropertyKey(property_key_1).make()
        index_prop_2 = self.mgmt.makePropertyKey(property_key_2).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Vertex") \
            .addKey(property_key_1).addKey(property_key_2).unique().indexOnly(constrained_label).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getUniqueness(), True)
        self.assertEqual(index.getConstrainedLabel(), label.getLabel())
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "VertexLabel")

        self.assertTrue(len(index.getProperties()) == len([index_prop_1, index_prop_2]))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_1[0].getName() for i in range(2)))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_2[0].getName() for i in range(2)))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_create_edge_composite_index_on_two_properties_constrained_by_label(self):
        # Fails. Unique index isn't working? Only on edge?
        index_name = "index10"
        property_key_1 = "index_prop13"
        property_key_2 = "index_prop14"
        constrained_label = "index_edge1"

        label = self.mgmt.makeEdgelabel(constrained_label).make()[0]
        index_prop_1 = self.mgmt.makePropertyKey(property_key_1).make()
        index_prop_2 = self.mgmt.makePropertyKey(property_key_2).make()

        index = self.mgmt.buildCompositeIndex(index_name).onElement("Edge") \
            .addKey(property_key_1).addKey(property_key_2).indexOnly(constrained_label).build()

        self.assertEqual(len(index), 1)
        index = index[0]

        self.assertEqual(index.getStatus(), "INSTALLED")
        self.assertEqual(index.getConstrainedLabel(), label.getLabel())
        self.assertEqual(index.getName(), index_name)
        self.assertEqual(index.getElementType(), "EdgeLabel")

        self.assertTrue(len(index.getProperties()) == len([index_prop_1, index_prop_2]))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_1[0].getName() for i in range(2)))
        self.assertTrue(any(index.getProperties()[i].getName() == index_prop_2[0].getName() for i in range(2)))

        time.sleep(10)
        idx = self.mgmt.enableCompositeIndex(index).enable()

        self.assertEqual(idx[0].getStatus(), "ENABLED")

    def test_get_all_composite_index_for_vertex(self):
        indices = self.mgmt.getVertexCompositeIndex("ALL")
        self.assertEqual(len(indices), 7)
        for i in indices:
            print(i)

    def test_get_all_composite_index_for_edge(self):
        indices = self.mgmt.getEdgeCompositeIndex("ALL")
        self.assertEqual(len(indices), 3)
    #
    # def test_vertex_index_retrieval_consistency(self):
    #     index_meta = {
    #         "index1": {
    #             "unique": False,
    #             "label": "ALL",
    #             "status": "REGISTERED",
    #             "properties": ["index_prop1"]
    #         },
    #         "index2": {
    #             "unique": False,
    #             "label": "ALL",
    #             "status": "ENABLED",
    #             "properties": ["index_prop2"]
    #         },
    #         "index5": {
    #             "unique": False,
    #             "label": "index_vertex1",
    #             "status": "ENABLED",
    #             "properties": ["index_prop5"]
    #         },
    #         "index6": {
    #             "unique": True,
    #             "label": "index_vertex2",
    #             "status": "ENABLED",
    #             "properties": ["index_prop6"]
    #         },
    #         "index7": {
    #             "unique": False,
    #             "label": "ALL",
    #             "status": "ENABLED",
    #             "properties": ["index_prop7", "index_prop8"]
    #         },
    #         "index8": {
    #             "unique": False,
    #             "label": "index_vertex3",
    #             "status": "ENABLED",
    #             "properties": ["index_prop9", "index_prop10"]
    #         },
    #         "index9": {
    #             "unique": True,
    #             "label": "index_vertex4",
    #             "status": "ENABLED",
    #             "properties": ["index_prop11", "index_prop12"]
    #         }
    #     }
    #
    #     for index_name, index_values in index_meta.items():
    #         index = self.mgmt.getVertexCompositeIndex(index_name)
    #
    #         self.assertEqual(len(index), 1)
    #         index = index[0]
    #
    #         self.assertEqual(index.getName(), index_name)
    #         self.assertEqual(index.getStatus(), index_values["status"])
    #         self.assertEqual(index.getConstrainedLabel(), index_values["label"])
    #         self.assertEqual(index.getUniqueness(), index_values["unique"])
    #         self.assertEqual(len(index.getProperties()), len(index_values["properties"]))
    #         self.assertTrue(any(index.getProperties()[i].getName() == index_values["properties"][i]
    #                             for i in range(len(index.getProperties()))))
    #     return
    #
    # def test_edge_index_retrieval_consistency(self):
    #     index_meta = {
    #         "index3": {
    #             "label": "ALL",
    #             "status": "REGISTERED",
    #             "properties": ["index_prop3"]
    #         },
    #         "index4": {
    #             "label": "ALL",
    #             "status": "ENABLED",
    #             "properties": ["index_prop4"]
    #         },
    #         "index10": {
    #             "label": "index_edge1",
    #             "status": "ENABLED",
    #             "properties": ["index_prop13", "index_prop14"]
    #         }
    #     }
    #     return


if __name__ == '__main__':
    unittest.main()

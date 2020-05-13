import unittest
from janusgraph.management.janusgraph_management import JanusGraphManagement
from janusgraph.management.element.property_key import PropertyKey


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.HOST = "localhost"
        self.PORT = 10182
        self.GRAPH = "graph_inmemory"

        self.mgmt = JanusGraphManagement().connect(self.HOST, self.PORT, self.GRAPH)

    def test_create_basic_property_key(self):
        property_key_name = "test1"

        properties = self.mgmt.makePropertyKey(property_key_name).make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]
        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), "String")
        self.assertEqual(property_key.getCardinality(), "Single")
        self.assertEqual(property_key.getConstrainedLabel(), "ALL")

          

    def test_create_geoshape_data_type_single_property_key(self):
        property_key_name = "test2"
        data_type = "GeoShape"

        properties = self.mgmt.makePropertyKey(property_key_name).dataType(data_type).make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]
        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), data_type)
        self.assertEqual(property_key.getCardinality(), "Single")
        self.assertEqual(property_key.getConstrainedLabel(), "ALL")

          

    def test_create_basic_property_list_cardinality_property_key(self):
        property_key_name = "test3"

        properties = self.mgmt.makePropertyKey(property_key_name).cardinality("List").make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]
        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), "String")
        self.assertEqual(property_key.getCardinality(), "List")
        self.assertEqual(property_key.getConstrainedLabel(), "ALL")

          

    def test_create_float_data_type_set_cardinality_property_key(self):
        property_key_name = "test4"
        cardinality = "Set"
        data_type = "Float64"

        properties = self.mgmt.makePropertyKey(property_key_name).cardinality("Set").dataType("Float64").make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]
        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), data_type)
        self.assertEqual(property_key.getCardinality(), cardinality)
        self.assertEqual(property_key.getConstrainedLabel(), "ALL")

          

    def test_create_basic_property_and_add_vertex_constrains(self):
        property_key_name = "test5"
        constrained_label = "vertex1"  # If the vertex doesn't exist, it gets added with defaults

        properties = self.mgmt.makePropertyKey(property_key_name).addVertexLabel(constrained_label).make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]

        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), "String")
        self.assertEqual(property_key.getCardinality(), "Single")
        self.assertEqual(property_key.getConstrainedLabel(), constrained_label)

        vertex = self.mgmt.getVertexLabel(constrained_label)[0]

        self.assertEqual(vertex.getPartitioned(), False)
        self.assertEqual(vertex.getStatic(), False)
        for x in vertex.getProperties():
            print(x)
        print("===")
        print(property_key)
        self.assertTrue(all(property_key.__str__() == x.__str__() for x in vertex.getProperties()))

          

    def test_create_basic_property_and_add_edge_constrains(self):
        property_key_name = "test6"
        constrained_label = "edge1"  # If the edge doesn't exist, it gets added with defaults

        properties = self.mgmt.makePropertyKey(property_key_name).addEdgeLabel(constrained_label).make()

        self.assertTrue(isinstance(properties, list))
        self.assertEqual(len(properties), 1)
        property_key = properties[0]

        self.assertTrue(isinstance(property_key, PropertyKey))
        self.assertEqual(property_key.getName(), property_key_name)
        self.assertEqual(property_key.getDataType(), "String")
        self.assertEqual(property_key.getConstrainedLabel(), constrained_label)

        edge = self.mgmt.getEdgeLabel(constrained_label)[0]

        print(property_key)
        print(edge.getProperties()[0])

        self.assertEqual(edge.getMultiplicity(), "Multi")
        self.assertEqual(edge.getDirected(), True)
        self.assertTrue(all(property_key.__str__() == x.__str__() for x in edge.getProperties()))

          

    def test_create_basic_property_and_multi_cardinality_for_edge_throw_exception(self):
        property_key_name = "test7"
        constrained_label = "testEdge"  # If the edge doesn't exist, it gets added with defaults

        self.assertRaises(Exception, self.mgmt.makePropertyKey(property_key_name).addEdgeLabel(constrained_label).cardinality("List").make, None)

    def test_constrained_property_throws_exception_on_wrong_data(self):
        property_key_name = "test8"
        constrained_label = "testVertex"  # If the vertex doesn't exist, it gets added with defaults

        self.assertRaises(Exception, self.mgmt.makePropertyKey(property_key_name).addVertexLabel(constrained_label).dataType, "RandomDataByte")

    def test_for_invalid_data_type(self):
        property_key_name = "test9"

        self.assertRaises(Exception, self.mgmt.makePropertyKey(property_key_name).dataType, "RandomDataByte")

    def test_for_invalid_cardinality(self):
        property_key_name = "test10"

        self.assertRaises(Exception, self.mgmt.makePropertyKey(property_key_name).cardinality, "Nothing")

    def test_for_undefined_label_in_property_key_retrieval(self):
        property_key_name = "test11"

        self.mgmt.makePropertyKey(property_key_name).make()

        properties = self.mgmt.getPropertyKey(property_key_name)[0]

        self.assertEqual(properties.getConstrainedLabel(), "UNDEFINED")

          

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_same_params(self):
        property_key_name = "test12"
        data_type = "Float32"
        cardinality = "List"
        constrained_label = "vertex2"

        self.mgmt.makePropertyKey(property_key_name).dataType(data_type).cardinality(cardinality).addVertexLabel(constrained_label).make()
        properties_made = self.mgmt.makePropertyKey(property_key_name).dataType(data_type).cardinality(cardinality).addVertexLabel(constrained_label).make()[0]

        properties_got = self.mgmt.getPropertyKey(property_key_name)
        self.assertEqual(1, len(properties_got))
        properties_got = properties_got[0]

        self.assertEqual(properties_got.getName(), properties_made.getName())
        self.assertEqual(properties_got.getDataType(), properties_made.getDataType())
        self.assertEqual(properties_got.getCardinality(), properties_made.getCardinality())
        # This will fail because when we do getPropertyKey, we can't get its constrained label and is UNDEFINED
        self.assertFalse(properties_got.getConstrainedLabel() == properties_made.getConstrainedLabel())

        property_key = self.mgmt.makePropertyKey(property_key_name).dataType(data_type).cardinality(cardinality).addVertexLabel(constrained_label).make()
        self.assertEqual(1, len(property_key))

        property_key = property_key[0]
        self.assertEqual(property_key.getName(), properties_made.getName())
        self.assertEqual(property_key.getDataType(), properties_made.getDataType())
        self.assertEqual(property_key.getCardinality(), properties_made.getCardinality())
        self.assertEqual(property_key.getConstrainedLabel(), properties_made.getConstrainedLabel())

        properties_got = self.mgmt.getPropertyKey(property_key_name)
        self.assertEqual(1, len(properties_got))
        properties_got = properties_got[0]

        self.assertEqual(properties_got.getName(), property_key.getName())
        self.assertEqual(properties_got.getDataType(), property_key.getDataType())
        self.assertEqual(properties_got.getCardinality(), property_key.getCardinality())

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_label(self):
        # Expects that the label constraint won't change and property gets
        # attached to first label it was constrained with

        property_key_name = "test13"
        data_type = "Float32"
        cardinality = "List"
        label1 = "vertex3"
        label2 = "vertex4"

        prop1 = self.mgmt.makePropertyKey(property_key_name).\
            dataType(data_type).cardinality(cardinality).addVertexLabel(label1).make()
        prop2 = self.mgmt.makePropertyKey(property_key_name).\
            dataType(data_type).cardinality(cardinality).addVertexLabel(label2).make()

        self.assertTrue(len(prop1) == len(prop2) == 1)
        prop1 = prop1[0]
        prop2 = prop2[0]

        self.assertEqual(prop1.getName(), prop2.getName())
        self.assertEqual(prop1.getDataType(), prop2.getDataType())
        self.assertEqual(prop1.getCardinality(), prop2.getCardinality())
        # This will fail because when we add property, since internally getPropertyKey()
        # doesn't have reflection of constrained label, we add the label it was constrained with
        self.assertFalse(prop1.getConstrainedLabel() == prop2.getConstrainedLabel())

        properties_got = self.mgmt.getPropertyKey(property_key_name)
        # Above query irrespective of running multiple times should have added only 1 propertyKey
        self.assertTrue(len(properties_got) == 1)
        properties_got = properties_got[0]

        # This should match with first definition of property
        self.assertEqual(properties_got.getName(), prop1.getName())
        self.assertEqual(properties_got.getDataType(), prop1.getDataType())
        self.assertEqual(properties_got.getCardinality(), prop1.getCardinality())
        self.assertEqual(properties_got.getConstrainedLabel(), "UNDEFINED")

        # Let's now retrieve vertex1 and see if it contains out constrained property
        vertex1 = self.mgmt.getVertexLabel(label1)

        self.assertEqual(len(vertex1), 1)
        vertex1 = vertex1[0]
        constrained_properties = vertex1.getProperties()

        self.assertEqual(len(constrained_properties), 1)
        constrained_properties = constrained_properties[0]
        # Now constrained_properties should be same as prop1
        self.assertEqual(constrained_properties.getName(), prop1.getName())
        self.assertEqual(constrained_properties.getDataType(), prop1.getDataType())
        self.assertEqual(constrained_properties.getCardinality(), prop1.getCardinality())
        self.assertEqual(constrained_properties.getConstrainedLabel(), prop1.getConstrainedLabel())

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_cardinality(self):
        property_key_name = "test14"
        data_type = "Float32"
        cardinality1 = "List"
        cardinality2 = "Single"
        label = "vertex5"

        prop1 = self.mgmt.makePropertyKey(property_key_name). \
            dataType(data_type).cardinality(cardinality1).addVertexLabel(label).make()

        prop2 = self.mgmt.makePropertyKey(property_key_name). \
            dataType(data_type).cardinality(cardinality2).addVertexLabel(label).make()

        self.assertTrue(len(prop1) == len(prop2) == 1)
        prop1 = prop1[0]
        prop2 = prop2[0]

        print(prop1)
        print(prop2)

        self.assertEqual(prop1.getName(), prop2.getName())
        self.assertEqual(prop1.getDataType(), prop2.getDataType())
        # Though we defined different cardinality, internally in server if property exists,
        # GET operation hence it will be equal
        self.assertEqual(prop1.getCardinality(), prop2.getCardinality())
        self.assertEqual(prop1.getConstrainedLabel(), prop2.getConstrainedLabel())

        properties_got = self.mgmt.getPropertyKey(property_key_name)
        # Above query irrespective of running multiple times should have added only 1 propertyKey
        self.assertTrue(len(properties_got) == 1)
        properties_got = properties_got[0]

        # This should match with first definition of property
        self.assertEqual(properties_got.getName(), prop1.getName())
        self.assertEqual(properties_got.getDataType(), prop1.getDataType())
        self.assertEqual(properties_got.getCardinality(), prop1.getCardinality())
        # Cardinality of prop2 should be different than the one we retrieved
        self.assertEqual(properties_got.getCardinality(), prop2.getCardinality())
        self.assertEqual(properties_got.getConstrainedLabel(), "UNDEFINED")

        # Nothing needed on VertexLabel Get side as we don't change constraints

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_data_type(self):
        # Since we redefine using different dataType, 2nd call is essentially redundant call
        # It does GET operation internally
        property_key_name = "test15"
        cardinality = "List"
        data_type1 = "Float32"
        data_type2 = "String"
        label = "vertex6"

        prop1 = self.mgmt.makePropertyKey(property_key_name). \
            dataType(data_type1).cardinality(cardinality).addVertexLabel(label).make()

        prop2 = self.mgmt.makePropertyKey(property_key_name). \
            dataType(data_type2).cardinality(cardinality).addVertexLabel(label).make()

        self.assertTrue(len(prop1) == len(prop2) == 1)
        prop1 = prop1[0]
        prop2 = prop2[0]

        print(prop1)
        print(prop2)

        self.assertEqual(prop1.getName(), prop2.getName())
        self.assertEqual(prop1.getDataType(), prop2.getDataType())
        # Though we defined different cardinality, internally in server if property exists,
        # GET operation hence it will be equal
        self.assertEqual(prop1.getCardinality(), prop2.getCardinality())
        self.assertEqual(prop1.getConstrainedLabel(), prop2.getConstrainedLabel())

        properties_got = self.mgmt.getPropertyKey(property_key_name)
        # Above query irrespective of running multiple times should have added only 1 propertyKey
        self.assertTrue(len(properties_got) == 1)
        properties_got = properties_got[0]

        # This should match with first definition of property
        self.assertEqual(properties_got.getName(), prop1.getName())
        self.assertEqual(properties_got.getDataType(), prop1.getDataType())
        self.assertEqual(properties_got.getCardinality(), prop1.getCardinality())
        # Cardinality of prop2 should be different than the one we retrieved
        self.assertEqual(properties_got.getCardinality(), prop2.getCardinality())
        self.assertEqual(properties_got.getConstrainedLabel(), "UNDEFINED")



if __name__ == '__main__':
    unittest.main()

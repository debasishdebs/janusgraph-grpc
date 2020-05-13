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
        property_key_name = "test"

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
        return

    def test_create_basic_property_list_cardinality_property_key(self):
        return

    def test_create_float_data_type_set_cardinality_property_key(self):
        return

    def test_create_basic_property_and_add_vertex_constrains(self):
        return

    def test_create_basic_property_and_add_edge_constrains(self):
        return

    def test_create_basic_property_and_multi_cardinality_for_edge_throw_exception(self):
        return

    def test_constrained_property_throws_exception_on_wrong_data(self):
        return

    def test_for_invalid_data_type(self):
        return

    def test_for_invalid_cardinality(self):
        return

    def test_for_undefined_label_in_property_key_retrieval(self):
        return

    def test_for_constrained_label_in_property_key_maker(self):
        return

    def test_for_unconstrained_label_in_property_key_maker(self):
        return

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_same_params(self):
        return

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_label(self):
        return

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_cardinality(self):
        return

    def test_behaviour_on_redoing_property_key_create_multiple_times_on_diff_data_type(self):
        return

    def test_get_property_keys_size_and_type(self):
        return

    def test_get_property_key_by_name_size_and_type(self):
        return

    def test_get_property_key_consistency(self):
        return


if __name__ == '__main__':
    unittest.main()

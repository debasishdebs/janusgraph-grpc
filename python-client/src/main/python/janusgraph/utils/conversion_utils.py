from collections.abc import Iterable

from janusgraph.management.element.vertex_label import VertexLabel
from janusgraph.management.element.edge_label import EdgeLabel
from janusgraph.management.element.property_key import PropertyKey


def is_custom_property_present(message, property_name):
    try:
        getattr(message, property_name)
        return True
    except AttributeError:
        return False


def convert_response_to_python_property_key(response):
    properties = []

    if isinstance(response, Iterable):
        for resp in response:
            property_key = PropertyKey(resp.name)
            property_key.set_id(resp.id)

            if is_custom_property_present(resp, "dataType"):
                property_key.set_data_type(resp.data_type)

            if is_custom_property_present(resp, "cardinality"):
                property_key.set_cardinality(resp.cardinality)

            properties.append(property_key)
    else:
        property_key = PropertyKey(response.name)
        property_key.set_id(response.id)

        if is_custom_property_present(response, "dataType"):
            property_key.set_data_type(response.data_type)

        if is_custom_property_present(response, "cardinality"):
            property_key.set_cardinality(response.cardinality)

        properties.append(property_key)

    return properties


def convert_response_to_python_vertex_label(response):
    vertices = []

    if isinstance(response, Iterable):
        for resp in response:
            vertex = VertexLabel(resp.name)
            vertex.set_id(resp.id)

            if is_custom_property_present(resp, "readOnly"):
                vertex.set_static(resp.readOnly)

            if is_custom_property_present(resp, "partitioned"):
                vertex.set_partitioned(resp.partitioned)

            if is_custom_property_present(resp, "properties"):
                vertex.set_properties(resp.properties)

            vertices.append(vertex)
    else:
        vertex = VertexLabel(response.name)
        vertex.set_id(response.id)

        if is_custom_property_present(response, "readOnly"):
            vertex.set_static(response.readOnly)

        if is_custom_property_present(response, "partitioned"):
            vertex.set_partitioned(response.partitioned)

        if is_custom_property_present(response, "properties"):
            vertex.set_properties(response.properties)

        vertices.append(vertex)
    return vertices


def convert_response_to_python_edge_label(response):
    edges = []

    if isinstance(response, Iterable):
        for resp in response:
            edge = EdgeLabel(resp.name)
            edge.set_id(resp.id)

            if is_custom_property_present(resp, "directed"):
                edge.set_directed(resp.directed)

            if is_custom_property_present(resp, "multiplicity"):
                edge.set_multiplicity(resp.multiplicity)

            if is_custom_property_present(resp, "properties"):
                edge.set_properties(resp.properties)

            if is_custom_property_present(resp, "direction"):
                edge.set_direction(resp.direction)

            edges.append(edge)
    else:
        edge = EdgeLabel(response.name)
        edge.set_id(response.id)

        if is_custom_property_present(response, "directed"):
            edge.set_directed(response.directed)

        if is_custom_property_present(response, "multiplicity"):
            edge.set_multiplicity(response.multiplicity)

        if is_custom_property_present(response, "properties"):
            edge.set_properties(response.properties)

        if is_custom_property_present(response, "direction"):
            edge.set_direction(response.direction)

        edges.append(edge)

    return edges

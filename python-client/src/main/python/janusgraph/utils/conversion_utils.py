from collections.abc import Iterable

from janusgraph.management.element.vertex_label import VertexLabel
from janusgraph.management.element.edge_label import EdgeLabel


def convert_response_to_python_vertex_label(response):
    vertices = []

    if isinstance(response, Iterable):
        for resp in response:
            vertex = VertexLabel(resp.name)
            vertex.set_id(resp.id)

            if "readOnly" in resp:
                vertex.set_static()

            if "partition" in resp:
                vertex.set_partitioned()

            if "properties" in resp:
                vertex.set_properties(resp.properties)

            vertices.append(vertex)
    else:
        vertex = VertexLabel(response.name)
        vertex.set_id(response.id)

        if "readOnly" in response:
            vertex.set_static()

        if "partition" in response:
            vertex.set_partitioned()

        if "properties" in response:
            vertex.set_properties(response.properties)

        vertices.append(vertex)
    return vertices


def convert_response_to_python_edge_label(response):
    edges = []

    if isinstance(response, Iterable):
        for resp in response:
            edge = EdgeLabel(resp.name)
            edge.set_id(resp.id)

            if "directed" in resp:
                edge.set_directed(resp.directed)

            if "multiplicity" in resp:
                edge.set_multiplicity(resp.multiplicity)

            if "properties" in resp:
                edge.set_properties(resp.properties)

            if "direction" in resp:
                edge.set_direction(resp.direction)

            edges.append(edge)
    else:
        edge = EdgeLabel(response.name)
        edge.set_id(response.id)

        if "directed" in response:
            edge.set_directed(response.directed)

        if "multiplicity" in response:
            edge.set_multiplicity(response.multiplicity)

        if "properties" in response:
            edge.set_properties(response.properties)

        if "direction" in response:
            edge.set_direction(response.direction)

        edges.append(edge)

    return edges

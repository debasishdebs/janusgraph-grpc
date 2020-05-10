import json
from janusgraph.utils.helpers import NpEncoder


class VertexLabel:
    ID = None
    LABEL = None
    STATIC = False
    PARTITIONED = False
    PROPERTIES = []

    def __init__(self, label):
        self.LABEL = label

    def set_static(self, is_static):
        self.STATIC = is_static
        return self

    def set_partitioned(self, is_partitioned):
        self.PARTITIONED = is_partitioned
        return self

    def set_id(self, ID):
        self.ID = ID.value

    def __str__(self):
        return json.dumps({"id": self.ID, "name": self.LABEL, "static": self.STATIC, "partitioned": self.PARTITIONED,
                "properties": self.PROPERTIES})

    def _check_if_valid_vertex_label_(self):
        if self.ID is None or self.LABEL is None:
            raise ValueError("ID and Label property needs to be defined to define a VertexLabel")

    def set_properties(self, properties):
        if len(properties) > 0:
            raise NotImplementedError("Not implemented setting properties value from response of gRPC server in VertexLabel")

    def get(self):
        self._check_if_valid_vertex_label_()
        return self

    def getLabel(self):
        return self.LABEL

    def getStatic(self):
        return self.STATIC

    def getPartitioned(self):
        return self.PARTITIONED

    def getProperties(self):
        return self.PROPERTIES

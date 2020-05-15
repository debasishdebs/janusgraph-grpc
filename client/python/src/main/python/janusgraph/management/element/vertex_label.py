import json
from janusgraph.management.element.property_key import PropertyKey


class VertexLabel:
    ID = None
    LABEL = None
    STATIC = False
    PARTITIONED = False
    PROPERTIES = {}

    def __init__(self, label):
        self.LABEL = label
        self.PROPERTIES = {}

    def set_static(self, is_static):
        self.STATIC = is_static
        return self

    def set_partitioned(self, is_partitioned):
        self.PARTITIONED = is_partitioned
        return self

    def set_id(self, ID):
        self.ID = ID.value

    def __str__(self):
        return str({'id': self.ID, 'name': self.LABEL, 'static': self.STATIC, 'partitioned': self.PARTITIONED,
                'properties': [str(x) for x in self.PROPERTIES.values()]})

    def _check_if_valid_vertex_label_(self):
        if self.ID is None or self.LABEL is None:
            raise ValueError("ID and Label property needs to be defined to define a VertexLabel")

    def set_properties(self, properties):
        for prop in properties:
            ID = prop.id
            property_key = PropertyKey(prop.name).set_id(ID).set_label(self.LABEL)

            property_key.set_data_type(getattr(prop, "dataType"))

            property_key.set_cardinality(getattr(prop, "cardinality"))

            if ID.value not in self.PROPERTIES:
                self.PROPERTIES[ID.value] = property_key

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
        return list(self.PROPERTIES.values())

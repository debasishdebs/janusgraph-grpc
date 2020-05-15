from janusgraph.management.element.property_key import PropertyKey
from typing import List


class CompositeIndex:
    ID = None
    NAME = None
    CONSTRAINED_LABEL = "ALL"
    UNIQUE = False
    ELEMENT_TYPE = None
    STATUS = "DISABLED"
    PROPERTIES = {}

    def __init__(self, name):
        self.NAME = name
        self.PROPERTIES = {}

    def set_uniqueness(self, unique_index):
        self.UNIQUE = unique_index
        return self

    def set_properties(self, properties):
        for prop in properties:
            ID = prop.id
            property_key = PropertyKey(prop.name).set_id(ID)

            property_key.set_data_type(getattr(prop, "dataType"))
            try:
                property_key.set_cardinality(getattr(prop, "cardinality"))
            except AttributeError:
                pass

            if ID.value not in self.PROPERTIES:
                self.PROPERTIES[ID.value] = property_key

    def set_label(self, label):
        self.CONSTRAINED_LABEL = label
        return self

    def set_id(self, ID):
        self.ID = ID.value
        return self

    def set_index_status(self, status):
        self.STATUS = status
        return self

    def set_element_type(self, element_type):
        self.ELEMENT_TYPE = element_type
        return self

    def __str__(self):
        return str({"id": self.ID, "name": self.NAME, "unique": self.UNIQUE, "status": self.STATUS,
                    "constrainedLabel": self.CONSTRAINED_LABEL, "properties": [str(x) for x in self.PROPERTIES.values()]})

    def _check_if_valid_property_key_(self):
        if self.ID is None or self.NAME is None:
            raise ValueError("ID and Label property needs to be defined to define a VertexLabel")

    def get(self):
        self._check_if_valid_property_key_()
        return self

    def getName(self):
        return self.NAME

    def getUniqueness(self):
        return self.UNIQUE

    def getStatus(self):
        return self.STATUS

    def getProperties(self) -> List[PropertyKey]:
        return list(self.PROPERTIES.values())

    def getConstrainedLabel(self):
        return self.CONSTRAINED_LABEL

    def getElementType(self):
        return self.ELEMENT_TYPE

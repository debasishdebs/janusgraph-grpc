import json


class PropertyKey:
    ID = None
    NAME = None
    DATA_TYPE = None
    CARDINALITY = None
    CONSTRAINED_LABEL = None

    def __init__(self, name):
        self.NAME = name

    def set_data_type(self, data_type):
        self.DATA_TYPE = data_type
        return self

    def set_cardinality(self, cardinality):
        self.CARDINALITY = cardinality
        return self

    def set_label(self, label):
        self.CONSTRAINED_LABEL = label
        return self

    def set_id(self, ID):
        self.ID = ID.value
        return self

    def __str__(self):
        return json.dumps({"id": self.ID, "name": self.NAME, "dataType": self.DATA_TYPE, "cardinality": self.CARDINALITY,
                           "constrainedLabel": self.CONSTRAINED_LABEL})

    def _check_if_valid_property_key_(self):
        if self.ID is None or self.NAME is None:
            raise ValueError("ID and Label property needs to be defined to define a VertexLabel")

    def get(self):
        self._check_if_valid_property_key_()
        return self

    def getName(self):
        return self.NAME

    def getDataType(self):
        return self.DATA_TYPE

    def getCardinality(self):
        return self.CARDINALITY

    def getConstrainedLabel(self):
        return self.CONSTRAINED_LABEL

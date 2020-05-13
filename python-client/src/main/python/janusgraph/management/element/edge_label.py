import json
from janusgraph_grpc_python.management.management_pb2 import EdgeLabel as Edge
from janusgraph.management.element.property_key import PropertyKey


class EdgeLabel:
    ID = None
    LABEL = None
    PROPERTIES = {}
    DIRECTED = True
    MULTIPLICITY = "Multi"
    DIRECTION = None

    def __init__(self, label):
        self.LABEL = label
        self.PROPERTIES = {}

    def set_directed(self, directed):
        directed_name = Edge.Directed.Name(directed)

        if directed_name == "directed_edge":
            self.DIRECTED = True
        else:
            self.DIRECTED = False
        return self

    def set_multiplicity(self, multiplicity):
        self.MULTIPLICITY = Edge.Multiplicity.Name(multiplicity)
        return self

    def set_id(self, ID):
        self.ID = ID.value

    def __str__(self):
        return str({"id": self.ID, "name": self.LABEL, "directed": self.DIRECTED, "multiplicity": self.MULTIPLICITY,
                "properties": [str(x) for x in self.PROPERTIES.values()], "direction": self.DIRECTION})

    def _check_if_valid_edge_label_(self):
        if self.ID is None or self.LABEL is None:
            raise ValueError("ID and Label property needs to be defined to define a EdgeLabel")

    def set_direction(self, direction):
        self.DIRECTION = Edge.Direction.Name(direction)

    def set_properties(self, properties):
        for prop in properties:
            ID = prop.id
            property_key = PropertyKey(prop.name).set_id(ID).set_label(self.LABEL)

            property_key.set_data_type(getattr(prop, "dataType"))

            if ID.value not in self.PROPERTIES:
                self.PROPERTIES[ID.value] = property_key

    def get(self):
        self._check_if_valid_edge_label_()
        return self

    def getLabel(self):
        return self.LABEL

    def getDirected(self):
        return self.DIRECTED

    def getMultiplicity(self):
        return self.MULTIPLICITY

    def getProperties(self):
        return list(self.PROPERTIES.values())

    def getDirection(self):
        return self.DIRECTION

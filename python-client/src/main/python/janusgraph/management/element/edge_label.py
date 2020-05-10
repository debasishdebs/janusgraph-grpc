class EdgeLabel:
    ID = None
    LABEL = None
    PROPERTIES = []
    DIRECTED = True
    MULTIPLICITY = "Multi"
    DIRECTION = None

    def __init__(self, label):
        self.LABEL = label

    def set_directed(self, direction_condition):
        if direction_condition == "directed_edge":
            self.DIRECTED = True
        else:
            self.DIRECTED = False
        return self

    def set_multiplicity(self, multiplicity):
        self.MULTIPLICITY = multiplicity
        return self

    def set_id(self, ID):
        self.ID = ID

    def __str__(self):
        return {"id": self.ID, "name": self.LABEL, "directed": self.DIRECTED, "multiplicity": self.MULTIPLICITY,
                "properties": self.PROPERTIES, "direction": self.DIRECTION}

    def _check_if_valid_edge_label_(self):
        if self.ID is None or self.LABEL is None:
            raise ValueError("ID and Label property needs to be defined to define a EdgeLabel")

    def set_direction(self, direction):
        raise NotImplementedError("Not implemented setting the direction of edge in EdgeLabel")

    def set_properties(self, properties):
        if len(properties) > 0:
            raise NotImplementedError("Not implemented setting properties value from response of gRPC server in EdgeLabel")

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
        return self.PROPERTIES

    def getDirection(self):
        return self.DIRECTED

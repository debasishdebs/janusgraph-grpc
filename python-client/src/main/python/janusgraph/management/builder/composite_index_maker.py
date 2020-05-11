from .schema_maker import SchemaMaker
from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType


class CompositeIndexMaker(SchemaMaker):
    ELEMENT = None
    METADATA = None
    LABEL = "ALL"

    OPERATION = "PUT"
    CHANNEL = None
    GRAPH = None

    PROPERTIES_TO_INDEX = []

    def __init__(self, name):
        super().__init__()

        self.INDEX_NAME = name
        self.METADATA = {
            "unique": False
        }

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def onElement(self, element_type):
        if element_type == "Vertex":
            self.ELEMENT = GraphElementType().set("VertexLabel")
        elif element_type == "Edge":
            self.ELEMENT = GraphElementType().set("EdgeLabel")
        else:
            raise AttributeError("The element to index will either be only Vertex or Edge but got " + element_type)
        return self

    def addKey(self, propertyKey):
        if isinstance(propertyKey, str):
            self.PROPERTIES_TO_INDEX.append(propertyKey)
        else:
            raise ValueError(f"Expecting propertyKey of type string but for {type(propertyKey)}")
        return self

    def unique(self):
        self.METADATA["unique"] = True
        return

    def indexOnly(self, label):
        self.LABEL = label
        return

    def build(self):
        return


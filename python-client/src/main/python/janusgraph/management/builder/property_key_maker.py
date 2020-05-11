from .schema_maker import SchemaMaker
from janusgraph_grpc_python.management.management_pb2 import PropertyDataType
from janusgraph_grpc_python.management.management_pb2 import VertexProperty
from janusgraph_grpc_python.graph_operation.command_action.graph_operation import GraphOperationMetadata
from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType
from janusgraph.utils.conversion_utils import convert_response_to_python_property_key


class PropertyKeyMaker(SchemaMaker):
    PROPERTY_NAME = None
    DATA_TYPE = "String"
    CARDINALITY = "SINGLE"
    ELEMENT_TYPE = "VertexLabel"
    LABEL = "ALL"

    ELEMENT = None

    OPERATION = "PUT"
    CHANNEL = None
    GRAPH = None

    allowed_data_types = ["String", "Character", "Boolean", "Int8", "Int16", "Int32", "Int64", "Float32", "Float64",
                          "Date", "JavaObject", "GeoShape", "Uuid"]

    allowed_cardinality = ["Single", "List", "Set"]

    def __init__(self, name):
        super(PropertyKeyMaker, self).__init__()

        self.PROPERTY_NAME = name

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def _check_if_valid_params_passed_(self):
        if self.PROPERTY_NAME is None:
            raise ValueError(f"PROPERTY_NAME needs to be provided as min param for create PropertyKey. "
                             f"Got {self.PROPERTY_NAME}")

        if self.CHANNEL is None:
            raise ValueError(f"set_channel() from JanusGraphManagement needs to be invoked before doing any operations")

        if self.GRAPH is None:
            raise ValueError("Call set_graph() before calling any operations")
        return self

    def _check_if_valid_data_type_passed_(self, data_type):
        if data_type not in self.allowed_data_types:
            raise AttributeError(f"Passed dataType {data_type} is not allowed dataType. "
                                 f"Allowed dataTypes are {self.allowed_data_types}")

    def _check_if_valid_cardinality_passed_(self, cardinality):
        if cardinality not in self.allowed_data_types:
            raise AttributeError(f"Passed Cardinality {cardinality} is not allowed Cardinality. "
                                 f"Allowed Cardinality are {self.allowed_cardinality}")

    def addConstraint(self, label):
        self.LABEL = label

    def dataType(self, data_type_string):
        self._check_if_valid_data_type_passed_(data_type_string)
        self.DATA_TYPE = PropertyDataType.Value(data_type_string)
        return self

    def cardinality(self, cardinality):
        self._check_if_valid_cardinality_passed_(cardinality)

        if cardinality == "Single":
            self.ELEMENT_TYPE = "EdgeLabel"

        self.CARDINALITY = VertexProperty.Cardinality.Value(cardinality)
        return self

    def _create_metadata_dict_(self):
        metadata = {
            "dataType": self.DATA_TYPE,
            "cardinality": self.CARDINALITY
        }
        return metadata

    def _create_element_(self):
        self.ELEMENT = GraphElementType().set(self.ELEMENT_TYPE)
        return self

    def make(self):
        self._check_if_valid_params_passed_()

        metadata = self._create_metadata_dict_()
        self._create_element_()

        metadata_object = GraphOperationMetadata().set_dict("ADDER", metadata)
        operation = self.make_operation(self.ELEMENT, self.LABEL, metadata_object)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)

        processor = operation.get_processor()

        return convert_response_to_python_property_key(processor.operate())

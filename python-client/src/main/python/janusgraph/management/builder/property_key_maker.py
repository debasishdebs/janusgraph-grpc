from .schema_maker import SchemaMaker
from janusgraph_grpc_python.management.management_pb2 import PropertyDataType
from janusgraph_grpc_python.management.management_pb2 import Cardinality
from janusgraph_grpc_python.graph_operation.command_action.graph_operation import GraphOperationMetadata
from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType
from janusgraph.utils.conversion_utils import convert_response_to_python_property_key
from janusgraph.management.builder.vertex_label_maker import VertexLabelMaker
from janusgraph.management.builder.edge_label_maker import EdgeLabelMaker


class PropertyKeyMaker(SchemaMaker):
    PROPERTY_NAME = None
    DATA_TYPE = "String"
    CARDINALITY = "Single"
    ELEMENT_TYPE = "PropertyKey"
    VERTEX_LABEL = "ALL"
    EDGE_LABEL = "ALL"
    LABEL = False

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
        if cardinality not in self.allowed_cardinality:
            raise AttributeError(f"Passed Cardinality {cardinality} is not allowed Cardinality. "
                                 f"Allowed Cardinality are {self.allowed_cardinality}")

    def addVertexLabel(self, label):
        if self.LABEL:
            raise ValueError(f"Label constraint already added for EdgeLabel {self.EDGE_LABEL}")

        self.VERTEX_LABEL = label
        self.LABEL = True
        return self

    def addEdgeLabel(self, label):
        if self.LABEL:
            raise ValueError(f"Label constraint already added for VertexLabel {self.VERTEX_LABEL}")

        self.EDGE_LABEL = label
        self.LABEL = True
        return self

    def dataType(self, data_type_string):
        self._check_if_valid_data_type_passed_(data_type_string)
        self.DATA_TYPE = data_type_string
        return self

    def cardinality(self, cardinality):
        self._check_if_valid_cardinality_passed_(cardinality)
        self.CARDINALITY = cardinality
        return self

    def _create_metadata_dict_(self):
        metadata = {
            "dataType": self.DATA_TYPE,
            "cardinality": self.CARDINALITY
        }

        # if self.LABEL:
        #     if self.VERTEX_LABEL != "ALL":
        #         metadata["label"] = self.VERTEX_LABEL
        #     else:
        #         metadata["label"] = self.EDGE_LABEL

        return metadata

    def _create_element_(self):
        self.ELEMENT = GraphElementType().set(self.ELEMENT_TYPE)
        return self

    def make(self):
        self._check_if_valid_params_passed_()

        # First we create property without label constraint
        metadata = self._create_metadata_dict_()
        self._create_element_()

        metadata_object = GraphOperationMetadata().set_dict("ADDER", metadata)
        operation = self.make_operation(element_type=self.ELEMENT, label=self.PROPERTY_NAME, metadata=metadata_object)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)

        processor = operation.get_processor()

        property_keys = convert_response_to_python_property_key(processor.operate())

        # Now we add/get a vertex defined by constraint and add property to it.
        if self.LABEL:
            label = self.VERTEX_LABEL if self.VERTEX_LABEL != "ALL" else self.EDGE_LABEL
            if self.EDGE_LABEL == "ALL":
                VertexLabelMaker(label).set_graph(self.GRAPH).set_channel(self.CHANNEL).\
                    setPropertyConstraint(self.PROPERTY_NAME).make()
            else:
                EdgeLabelMaker(label).set_graph(self.GRAPH).set_channel(self.CHANNEL).\
                    setPropertyConstraint(self.PROPERTY_NAME).make()

                property_keys = convert_response_to_python_property_key(processor.operate(), element="EdgeLabel")

            for property_key in property_keys:
                property_key.set_label(label)
        print(property_keys)
        return property_keys


class PropertyKeyGetter(SchemaMaker):
    NAME = None
    ELEMENT = None

    OPERATION = "GET"
    CHANNEL = None
    GRAPH = None

    def __init__(self, name=None):
        super().__init__()
        self.NAME = name

        self.ELEMENT = GraphElementType().set("PropertyKey")

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def _prepare_query_(self):
        metadata = GraphOperationMetadata().set_dict("ADDER", {})

        if self.NAME is not None:
            operation = self.make_operation(self.ELEMENT, self.NAME, metadata)
        else:
            operation = self.make_operation(self.ELEMENT, "ALL", metadata)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)
        return operation

    def _check_if_valid_params_passed_(self):
        if self.CHANNEL is None:
            raise ValueError(f"set_channel() from JanusGraphManagement needs to be invoked before doing any operations")

        if self.GRAPH is None:
            raise ValueError("Call set_graph() before calling any operations")
        return self

    def get(self):
        self._check_if_valid_params_passed_()

        operation = self._prepare_query_()

        processor = operation.get_processor()

        property_keys = convert_response_to_python_property_key(processor.operate())

        return [x.set_label("UNDEFINED") for x in property_keys]

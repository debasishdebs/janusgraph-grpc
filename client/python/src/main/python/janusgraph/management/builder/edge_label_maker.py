from .schema_maker import SchemaMaker

from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType
from janusgraph_grpc_python.graph_operation.command_action.graph_operation_metadata import GraphOperationMetadata
from janusgraph.utils.conversion_utils import convert_response_to_python_edge_label


class EdgeLabelMaker(SchemaMaker):
    LABEL = None

    ELEMENT = None
    METADATA = {}

    OPERATION = "PUT"
    CHANNEL = None
    GRAPH = None

    multiplicity_values = ["Simple", "Many2One", "Multi", "One2Many", "One2One"]

    def __init__(self, label):
        super().__init__()

        self.METADATA = dict()

        self.LABEL = label

        self.ELEMENT = GraphElementType().set("EdgeLabel")

    def set_channel(self, channel):
        self.CHANNEL = channel
        return self

    def set_graph(self, graph):
        self.GRAPH = graph
        return self

    def _check_if_valid_params_passed_(self):
        if self.LABEL is None:
            raise ValueError(f"LABEL needs to be provided as min param for create VertexLabel. Got {self.LABEL}")

        if self.CHANNEL is None:
            raise ValueError(f"set_channel() from JanusGraphManagement needs to be invoked before doing any operations")

        if self.GRAPH is None:
            raise ValueError("Call set_graph() before calling any operations")
        return self

    def multiplicity(self, mult):
        if mult in self.multiplicity_values:
            self.METADATA["multiplicity"] = mult
        else:
            raise AttributeError(f"Invalid multiplicity passed = {mult} expecting among {self.multiplicity_values}")
        return self

    def directed(self):
        self.METADATA["directed"] = True
        return self

    def undirected(self):
        self.METADATA["directed"] = False
        return self

    def setPropertyConstraint(self, edge_property):
        self.METADATA["properties"] = edge_property
        print("Not implemented adding VertexProperty constraint to EdgeLabel")
        return self

    def make(self):
        self._check_if_valid_params_passed_()

        metadata = GraphOperationMetadata().set_dict("ADDER", self.METADATA)
        operation = self.make_operation(self.ELEMENT, self.LABEL, metadata)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)

        processor = operation.get_processor()

        return convert_response_to_python_edge_label(processor.operate())


class EdgeLabelGetter(SchemaMaker):
    LABEL = None
    ELEMENT = None

    OPERATION = "GET"
    CHANNEL = None
    GRAPH = None

    def __init__(self, label=None):
        super().__init__()
        self.LABEL = label

        self.ELEMENT = GraphElementType().set("EdgeLabel")

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def _prepare_query_(self):
        metadata = GraphOperationMetadata().set_dict("ADDER", {})

        if self.LABEL is not None:
            operation = self.make_operation(self.ELEMENT, self.LABEL, metadata)
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

        return convert_response_to_python_edge_label(processor.operate())
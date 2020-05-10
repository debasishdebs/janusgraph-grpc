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

    multiplicity_values = ["Simple", "Many2One", "Multi", "One2Many", "One2One"]

    def __init__(self, label):
        super().__init__()

        self.LABEL = label

        self.ELEMENT = GraphElementType().set("EdgeLabel")

    def set_channel(self, channel):
        self.CHANNEL = channel

    def _check_if_valid_params_passed_(self):
        if self.LABEL is None:
            raise ValueError(f"LABEL needs to be provided as min param for create VertexLabel. Got {self.LABEL}")

        if self.CHANNEL is None:
            raise ValueError(f"set_channel() from JanusGraphManagement needs to be invoked before doing any operations")
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

    def setProperty(self, edge_property):
        raise NotImplementedError("Not implemented adding VertexProperty constraint to VertexLabel")

    def make(self):
        self._check_if_valid_params_passed_()

        metadata = GraphOperationMetadata().set_dict("ADDER", self.METADATA)
        operation = self.make_operation(self.ELEMENT, self.LABEL, metadata)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)

        processor = operation.get_processor()

        return convert_response_to_python_edge_label(processor.operate())

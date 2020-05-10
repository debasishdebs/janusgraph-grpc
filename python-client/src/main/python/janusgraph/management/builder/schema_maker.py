from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType
from janusgraph_grpc_python.graph_operation.command_action.graph_operation_metadata import GraphOperationMetadata
from janusgraph_grpc_python.graph_operation.command_action.graph_operation import GraphOperation


class SchemaMaker:
    def __init__(self):
        pass

    def make_operation(self, element_type, label, metadata):
        """

        Args:
            element_type (GraphElementType):
            label (str):
            metadata (GraphOperationMetadata):

        Returns:

        """

        operation = GraphOperation(element_type, label, metadata)

        return operation

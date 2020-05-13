from .graph_element import GraphElement
from janusgraph_grpc_python.management import management_pb2
from janusgraph_grpc_python.graph_operation.graph_indexer import GraphIndexer
from janusgraph_grpc_python.graph_operation.graph_adder import GraphElementAdder


class Vertex(GraphElement):
    def __init__(self, operation, label, optional_metadata=None):
        super().__init__("VertexLabel", operation, label, optional_metadata)

        self.operation = operation
        self.element_label = label

        self.CONTEXT = None
        self.REQUEST = None
        self.ELEMENT = None
        self.OPTIONAL_OPERATOR = None
        self.OPTIONAL_METADATA = optional_metadata

        if self.element_label is not "ALL":
            self.ELEMENT = management_pb2.VertexLabel(name=self.element_label)

    def get_element(self):
        return self.ELEMENT

    def set_optional_operator(self, addtnl_operator):
        self.OPTIONAL_OPERATOR = addtnl_operator

    def __generate_context__(self):
        if self.GRAPH_NAME is not None:
            self.CONTEXT = management_pb2.JanusGraphContext(graphName=self.GRAPH_NAME)
        else:
            raise ValueError("Please call set_graph_name on graph_element() before generating context")
        return self

    def __generate_request__(self):
        if self.operation == "GET":
            if self.element_label == "ALL":
                self.REQUEST = management_pb2.GetVertexLabelsRequest(context=self.CONTEXT)
            else:
                self.REQUEST = management_pb2.GetVertexLabelsByNameRequest(context=self.CONTEXT, name=self.element_label)
        else:
            if self.element_label is not "ALL":
                self.REQUEST = management_pb2.EnsureVertexLabelRequest(context=self.CONTEXT, label=self.ELEMENT)
            else:
                raise NotImplementedError("Implemented PUT operation on VertexLabel when "
                                          "a vertexLabel name is provided not when ALL")
        return self

    def __get__(self):
        self.__generate_context__()
        self.__generate_request__()

        if self.OPTIONAL_METADATA is None:
            if self.element_label == "ALL":
                elem = self.service.GetVertexLabels(self.REQUEST)
            else:
                elem = self.service.GetVertexLabelsByName(self.REQUEST)

            return elem
        else:
            self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

            if isinstance(self.OPTIONAL_OPERATOR, GraphIndexer):
                if self.element_label == "ALL":
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.get_all_indices()

                else:
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.get_indices_by_label()

            else:
                raise NotImplementedError("Not implemented GET method for GraphAdder instance. "
                                          "Because logically different")

    def __put__(self):
        self.__generate_context__()
        self.__generate_request__()
        if self.OPTIONAL_METADATA is None:
            return self.service.EnsureVertexLabel(self.REQUEST)

        else:
            if isinstance(self.OPTIONAL_OPERATOR, GraphIndexer):
                self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

                if self.element_label == "ALL":
                    raise NotImplementedError("Not yet implemented PUT operation on index with ALL VertexLabel. TODO")
                    pass
                else:
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()

                    return indexer.put_index()
            elif isinstance(self.OPTIONAL_OPERATOR, GraphElementAdder):
                self.ELEMENT = self.OPTIONAL_OPERATOR.get_element()
                self.__generate_request__()

                print("Not yet implemented PUT method for GraphAdder instance in VertexLabel. "
                      "--TODO--[Case when Vertex is added without defaults]")

                return self.service.EnsureVertexLabel(self.REQUEST)

            else:
                raise ValueError("Invalid graph operator got. Expecting either of GraphIndexer of GraphElementAdder")

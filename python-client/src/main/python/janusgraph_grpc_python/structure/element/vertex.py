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

        # if self.element_label is not "ALL":
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
        elif self.operation == "PUT":
            if self.element_label is not "ALL":
                self.REQUEST = management_pb2.EnsureVertexLabelRequest(context=self.CONTEXT, label=self.ELEMENT)
            else:
                raise NotImplementedError("Implemented PUT operation on VertexLabel when "
                                      "a vertexLabel name is provided not when ALL")
        else:
            raise NotImplementedError("Implemented only GET/PUT in generate_request in vertex.py")
        return self

    def __get__(self):
        is_indexing_operation = isinstance(self.OPTIONAL_OPERATOR, GraphIndexer)

        self.__generate_context__()

        if self.OPTIONAL_METADATA is None:
            self.__generate_request__()
            if self.element_label == "ALL":
                elem = self.service.GetVertexLabels(self.REQUEST)
            else:
                elem = self.service.GetVertexLabelsByName(self.REQUEST)

            return elem
        else:
            self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

            if is_indexing_operation:
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
        is_indexing_operation = isinstance(self.OPTIONAL_OPERATOR, GraphIndexer)
        is_addition_operation = isinstance(self.OPTIONAL_OPERATOR, GraphElementAdder)

        self.__generate_context__()

        if self.OPTIONAL_METADATA is None:
            self.__generate_request__()
            return self.service.EnsureVertexLabel(self.REQUEST)

        else:
            if is_indexing_operation:
                self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

                if self.element_label == "ALL":
                    print("Not yet implemented PUT operation on index with ALL VertexLabel. TODO")
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.put_index_across_all_labels()

                else:
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.put_index_by_label()

            elif is_addition_operation:
                self.ELEMENT = self.OPTIONAL_OPERATOR.get_element()
                self.__generate_request__()

                print("Not yet implemented PUT method for GraphAdder instance in VertexLabel. "
                      "--TODO--[Case when Vertex is added without defaults]")

                return self.service.EnsureVertexLabel(self.REQUEST)

            else:
                raise ValueError("Invalid graph operator got. Expecting either of GraphIndexer of GraphElementAdder")

    def __enable__(self):
        # This is called only during Indexing operation to Enable an index
        # Metadata will NOT BE NONE
        # Additional Operator will be Indexer. element_label != ALL
        self.__generate_context__()
        self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

        if self.element_label == "ALL":
            print("ELEMENT_LABEL is ALL in enabling index. Ignoring it though")
            indexer = self.OPTIONAL_OPERATOR.get_indexer()
            return indexer.enable_index_by_name()

        else:
            raise NotImplementedError("Not Implemented enabling all indices specific to a label")

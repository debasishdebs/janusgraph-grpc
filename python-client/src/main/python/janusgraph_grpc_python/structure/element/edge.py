from .graph_element import GraphElement
from janusgraph_grpc_python.management import management_pb2
from janusgraph_grpc_python.graph_operation.graph_indexer import GraphIndexer
from janusgraph_grpc_python.graph_operation.graph_adder import GraphElementAdder


class Edge(GraphElement):
    def __init__(self, operation, label, optional_metadata=None):
        super().__init__("EdgeLabel", operation, label, optional_metadata)

        self.operation = operation
        self.element_label = label

        self.CONTEXT = None
        self.REQUEST = None
        self.ELEMENT = None
        self.OPTIONAL_OPERATOR = None
        self.OPTIONAL_METADATA = optional_metadata

        # if self.element_label is not "ALL":
        self.ELEMENT = management_pb2.EdgeLabel(name=self.element_label)

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
                self.REQUEST = management_pb2.GetEdgeLabelsRequest(context=self.CONTEXT)
            else:
                self.REQUEST = management_pb2.GetEdgeLabelsByNameRequest(context=self.CONTEXT, name=self.element_label)
        else:
            if self.element_label is not "ALL":
                self.REQUEST = management_pb2.EnsureEdgeLabelRequest(context=self.CONTEXT, label=self.ELEMENT)
            else:
                raise NotImplementedError("Implemented PUT operation on EdgeLabel when "
                                          "a vertexLabel name is provided not when ALL")
        return self

    def __get__(self):
        self.__generate_context__()
        self.__generate_request__()

        if self.OPTIONAL_METADATA is None:
            if self.element_label == "ALL":
                return self.service.GetEdgeLabels(self.REQUEST)
            else:
                return self.service.GetEdgeLabelsByName(self.REQUEST)
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
        print("I'm inside put of " + str(self))
        print("Element label is " + self.element_label)
        print("Optional operator is " + str(self.OPTIONAL_OPERATOR))

        self.__generate_context__()

        if self.OPTIONAL_METADATA is None:
            self.__generate_request__()
            return self.service.EnsureEdgeLabel(self.REQUEST)

        else:
            if isinstance(self.OPTIONAL_OPERATOR, GraphIndexer):
                self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

                if self.element_label == "ALL":
                    print("Not yet implemented PUT operation on index with ALL EdgeLabel. TODO")
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.put_index_across_all_labels()
                else:
                    indexer = self.OPTIONAL_OPERATOR.get_indexer()
                    return indexer.put_index_by_label()

            elif isinstance(self.OPTIONAL_OPERATOR, GraphElementAdder):
                self.ELEMENT = self.OPTIONAL_OPERATOR.get_element()
                self.__generate_request__()

                print("Not yet implemented PUT method for GraphAdder instance in EdgeLabel. "
                      "--TODO--[Case when Vertex is added without defaults]")

                return self.service.EnsureEdgeLabel(self.REQUEST)

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

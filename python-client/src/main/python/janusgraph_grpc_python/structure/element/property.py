from .graph_element import GraphElement
from janusgraph_grpc_python.management import management_pb2
from janusgraph_grpc_python.graph_operation.graph_indexer import GraphIndexer
from janusgraph_grpc_python.graph_operation.graph_adder import GraphElementAdder


class Property(GraphElement):
    def __init__(self, operation, label, optional_metadata=None):
        super().__init__("PropertyKey", operation, label, optional_metadata)

        self.operation = operation
        self.element_label = label

        self.CONTEXT = None
        self.REQUEST = None
        self.ELEMENT = None
        self.OPTIONAL_OPERATOR = None
        self.OPTIONAL_METADATA = optional_metadata

        if self.element_label is not "ALL":
            self.ELEMENT = management_pb2.PropertyKey(name=self.element_label)
        else:
            raise AttributeError("PropertyKey can't be called with <label> as ALL")

    def get_element(self):
        print(f"Getting the get_element() with value = {self.ELEMENT} and the class is {type(self.ELEMENT)}")
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
                self.REQUEST = management_pb2.GetPropertyKeysRequest(context=self.CONTEXT)
            else:
                self.REQUEST = management_pb2.GetPropertyKeyByNameRequest(context=self.CONTEXT, name=self.element_label)
        else:
            if self.element_label is not "ALL":
                print(self.ELEMENT)
                self.REQUEST = management_pb2.EnsurePropertyKeyRequest(context=self.CONTEXT, property=self.ELEMENT)
            else:
                raise NotImplementedError("Implemented PUT operation on PropertyKey when "
                                          "a vertexLabel name is provided not when ALL")
        return self

    def __get__(self):
        self.__generate_context__()
        self.__generate_request__()

        if self.OPTIONAL_METADATA is None:

            if self.element_label == "ALL":
                elem = self.service.GetPropertyKeys(self.REQUEST)
            else:
                elem = self.service.GetPropertyKeyByName(self.REQUEST)

            return elem
        else:
            self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)

            if isinstance(self.OPTIONAL_OPERATOR, GraphIndexer):
                raise NotImplementedError("TODO: Implement all Indices when PropertyKey is defined")
                # if self.element_label == "ALL":
                #     indexer = self.OPTIONAL_OPERATOR.get_indexer()
                #     return indexer.get_all_indices()
                #
                # else:
                #     indexer = self.OPTIONAL_OPERATOR.get_indexer()
                #     return indexer.get_indices_by_label()

            else:
                raise NotImplementedError("Not implemented GET method for GraphAdder instance. "
                                          "Because logically different")

    def __put__(self):
        self.__generate_context__()
        self.__generate_request__()
        if self.OPTIONAL_METADATA is None:
            return self.service.EnsurePropertyKey(self.REQUEST)

        else:
            if isinstance(self.OPTIONAL_OPERATOR, GraphIndexer):
                raise NotImplementedError("TODO: Implement adding Index when PropertyKey is defined")
                # self.OPTIONAL_OPERATOR.set_context(self.CONTEXT)
                #
                # if self.element_label == "ALL":
                #     raise NotImplementedError("Not yet implemented PUT operation on index with ALL PropertyKey. TODO")
                #     pass
                # else:
                #     indexer = self.OPTIONAL_OPERATOR.get_indexer()
                #
                #     return indexer.put_index()
            elif isinstance(self.OPTIONAL_OPERATOR, GraphElementAdder):
                self.ELEMENT = self.OPTIONAL_OPERATOR.get_element()
                self.__generate_request__()

                print(self.ELEMENT)
                print(self.ELEMENT.dataType, self.ELEMENT.cardinality, self.ELEMENT.name)

                print("Not yet implemented PUT method for GraphAdder instance in PropertyKey. "
                      "--TODO--[Case when Vertex is added without defaults]")

                return self.service.EnsurePropertyKey(self.REQUEST)

            else:
                raise ValueError("Invalid graph operator got. Expecting either of GraphIndexer of GraphElementAdder")
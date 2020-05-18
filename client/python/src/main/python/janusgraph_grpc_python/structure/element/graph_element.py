class GraphElement(object):
    GRAPH_NAME = None

    def __init__(self, element, operation, metadata, optional_metadata):
        self.element = element
        self.operation = operation
        self.metadata = metadata
        self.service = None

        self.OPTIONAL_METADATA = optional_metadata

    def set_graph_name(self, graph_name):
        self.GRAPH_NAME = graph_name
        return self

    def get_element(self):
        raise NotImplementedError("Not implemented GraphElement without being subclassed and get_element() being called")

    def set_service(self, stub):
        self.service = stub

    def operate(self):

        if self.service is None:
            raise ValueError("Please call set_service(stub) before calling operate()")

        if self.operation == "GET":
            return self.__get__()
        elif self.operation == "PUT":
            return self.__put__()
        elif self.operation == "ENABLE":
            return self.__enable__()
        else:
            raise NotImplementedError("Implemented only GET, PUT and ENABLE operations till now")

    def __get__(self):
        pass

    def __put__(self):
        return

    def __enable__(self):
        raise NotImplementedError("Not subclassed enable() method yet from graph_element")

    def __str__(self):
        return self.element
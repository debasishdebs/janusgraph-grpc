from .schema_maker import SchemaMaker
from janusgraph_grpc_python.type_class.graph_element_type import GraphElementType
from janusgraph_grpc_python.graph_operation.command_action.graph_operation_metadata import GraphOperationMetadata
from janusgraph.utils.conversion_utils import convert_response_to_python_composite_index
from janusgraph.management.element.composite_index import CompositeIndex
from typing import List


class CompositeIndexMaker(SchemaMaker):
    ELEMENT = None
    METADATA = None
    LABEL = "ALL"

    OPERATION = "PUT"
    CHANNEL = None
    GRAPH = None

    INSTALLED = False

    PROPERTIES_TO_INDEX = []

    def __init__(self, name):
        super().__init__()

        self.INDEX_NAME = name
        self.METADATA = {
            "unique": False
        }
        self.PROPERTIES_TO_INDEX = []

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def onElement(self, element_type):
        if element_type == "Vertex":
            self.ELEMENT = GraphElementType().set("VertexLabel")
        elif element_type == "Edge":
            self.ELEMENT = GraphElementType().set("EdgeLabel")
        else:
            raise AttributeError("The element to index will either be only Vertex or Edge but got " + element_type)
        return self

    def addKey(self, propertyKey):
        if isinstance(propertyKey, str):
            self.PROPERTIES_TO_INDEX.append(propertyKey)
        else:
            raise ValueError(f"Expecting propertyKey of type string but for {type(propertyKey)}")
        return self

    def unique(self):
        self.METADATA["unique"] = True
        return self

    def indexOnly(self, label):
        self.LABEL = label
        return self

    def _create_metadata_dict_(self):
        print("creating metadata")
        if self.LABEL == "ALL":
            metadata = {
                "index_type": "CompositeIndex",
                "index_name": self.INDEX_NAME,
                "index_on": self.PROPERTIES_TO_INDEX,
                "unique_index": self.METADATA["unique"]

            }
        else:
            metadata = {
                "index_type": "CompositeIndex",
                "index_name": self.INDEX_NAME,
                "index_on": self.PROPERTIES_TO_INDEX,
                "index_only": self.LABEL,
                "unique_index": self.METADATA["unique"]

            }

        return metadata

    def _check_if_valid_params_passed_(self):
        if self.INDEX_NAME is None:
            raise ValueError(f"INDEX_NAME needs to be provided as min param for create CompositeKey. "
                             f"Got {self.INDEX_NAME}")

        if self.CHANNEL is None:
            raise ValueError(f"set_channel() from JanusGraphManagement needs to be invoked before doing any operations")

        if self.GRAPH is None:
            raise ValueError("Call set_graph() before calling any operations")
        return self

    def enableIndex(self):
        if not self.INSTALLED:
            raise AttributeError("Please INSTALL the index first calling build() before enabling index")

        metadata = self._create_metadata_dict_()
        metadata_object = GraphOperationMetadata().set_dict("INDEX", metadata)

        operation = self.make_operation(element_type=self.ELEMENT, label=self.LABEL, metadata=metadata_object)

        operation.set_operation("ENABLE")
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)

        processor = operation.get_processor()

        return convert_response_to_python_composite_index(processor.operate(), None)

    def build(self):
        self._check_if_valid_params_passed_()

        metadata = self._create_metadata_dict_()
        print("Metadata is ")
        print(metadata)

        metadata_object = GraphOperationMetadata().set_dict("INDEX", metadata)

        operation = self.make_operation(element_type=self.ELEMENT, label=self.LABEL, metadata=metadata_object)

        operation.set_operation(self.OPERATION)
        operation.set_channel(self.CHANNEL)
        operation.set_graph_name(self.GRAPH)

        processor = operation.get_processor()

        print("Got processor for Composite Index for " + self.LABEL)
        print(processor)
        print("Operating")
        print("====END=====")

        index = convert_response_to_python_composite_index(processor.operate(), str(self.ELEMENT))

        if index[0].getStatus() == "INSTALLED":
            self.INSTALLED = True

        return index


class CompositeIndexGetter(SchemaMaker):
    LABEL = None
    NAME = None
    ELEMENT = None

    OPERATION = "GET"
    CHANNEL = None
    GRAPH = None

    def __init__(self, element):
        super().__init__()
        self.ELEMENT = GraphElementType().set(element)

    def set_channel(self, channel):
        self.CHANNEL = channel

    def set_graph(self, graph):
        self.GRAPH = graph

    def restrict_label(self, label):
        self.LABEL = label
        return self

    def restrict_name(self, name):
        self.NAME = name
        return self

    def _prepare_query_(self):

        if self.NAME is None and self.LABEL is None:
            metadata = GraphOperationMetadata().set_dict("INDEX", {"index_type": "CompositeIndex"})
            # Meaning we are going to fetch all composite indices
            operation = self.make_operation(self.ELEMENT, "ALL", metadata)
        else:
            if self.NAME is None:
                # Meaning we are going to fetch for a label
                metadata = GraphOperationMetadata().set_dict("INDEX", {"index_type": "CompositeIndex"})
                operation = self.make_operation(self.ELEMENT, self.LABEL, metadata)
            elif self.LABEL is None:
                # Meaning we are going to fetch composite indices by name
                metadata = GraphOperationMetadata().set_dict("INDEX", {"index_name": self.NAME, "index_type": "CompositeIndex"})
                operation = self.make_operation(self.ELEMENT, self.NAME, metadata)
            else:
                raise NotImplementedError("Not implemented filtering getCompositeIndex by both label and name")

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

    def get(self) -> List[CompositeIndex]:
        self._check_if_valid_params_passed_()

        operation = self._prepare_query_()

        processor = operation.get_processor()

        return convert_response_to_python_composite_index(processor.operate(), self.ELEMENT)


class CompositeIndexEnabler(SchemaMaker):
    INDEX = None

    def __init__(self):
        super().__init__()
        self.METADATA = {}

    def set_index(self, index):
        """

        Args:
            index (CompositeIndex):

        Returns:

        """
        self.INDEX = index
        return self

    def set_channel(self, channel):
        self.CHANNEL = channel
        return self

    def set_graph(self, graph):
        self.GRAPH = graph
        return self

    def _set_metadata_dict_(self):
        self.METADATA["index_type"] = "CompositeIndex"
        self.METADATA["index_name"] = self.INDEX.getName()
        self.METADATA["index_on"] = self.INDEX.getProperties()
        self.METADATA["element_to_index"] = self.INDEX.getElementType()
        return self

    def enable(self):
        if self.INDEX is None:
            raise ValueError("Please call set_idex() befonal calling enable()")

        if self.INDEX.getStatus() == "INSTALLED":
            self._set_metadata_dict_()
            metadata_object = GraphOperationMetadata().set_dict("INDEX", self.METADATA)

            ELEMENT = GraphElementType().set(self.METADATA["element_to_index"])

            operation = self.make_operation(element_type=ELEMENT, label="ALL", metadata=metadata_object)

            operation.set_operation("ENABLE")
            operation.set_channel(self.CHANNEL)
            operation.set_graph_name(self.GRAPH)

            processor = operation.get_processor()

            return convert_response_to_python_composite_index(processor.operate(), self.METADATA["element_to_index"])

        else:
            print("ENABLE_INDEX can't be called because INDEX is in " + self.INDEX.getStatus() + " state")

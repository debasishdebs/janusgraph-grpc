import grpc

from janusgraph.management.builder.vertex_label_maker import VertexLabelMaker, VertexLabelGetter
from janusgraph.management.builder.edge_label_maker import EdgeLabelMaker, EdgeLabelGetter
from janusgraph.management.builder.property_key_maker import PropertyKeyMaker, PropertyKeyGetter
from janusgraph.management.builder.composite_index_maker import CompositeIndexMaker, CompositeIndexEnabler, CompositeIndexGetter
from janusgraph.management.element.composite_index import CompositeIndex


class JanusGraphManagement:
    HOST = None
    PORT = None
    GRAPH = None
    CHANNEL = None

    def __init__(self):
        pass

    def connect(self, host, port, graph):
        self.HOST = host
        self.PORT = port
        self.GRAPH = graph

        self._create_channel_()

        return self

    def close(self):
        self.CHANNEL.close()

    def _create_channel_(self):
        self.CHANNEL = grpc.insecure_channel(f'{self.HOST}:{self.PORT}')
        return self

    def _check_if_connection_is_established_(self):
        if self.HOST is None or self.PORT is None or self.GRAPH is None or self.CHANNEL is None:
            raise ValueError("Please call connect() before calling any make() instances")

    def makePropertyKey(self, name) -> PropertyKeyMaker:
        """

        Args:
            name (str):

        Returns:
            PropertyKeyMaker: PropertyKeyMaker object to create property keys
        """

        maker = PropertyKeyMaker(name)
        maker.set_channel(self.CHANNEL)
        maker.set_graph(self.GRAPH)

        return maker

    def makeVertexLabel(self, label) -> VertexLabelMaker:
        self._check_if_connection_is_established_()

        maker = VertexLabelMaker(label)
        maker.set_channel(self.CHANNEL)
        maker.set_graph(self.GRAPH)

        return maker

    def makeEdgelabel(self, label) -> EdgeLabelMaker:
        self._check_if_connection_is_established_()

        maker = EdgeLabelMaker(label)
        maker.set_channel(self.CHANNEL)
        maker.set_graph(self.GRAPH)

        return maker

    def getPropertyKey(self, name):
        self._check_if_connection_is_established_()

        getter = PropertyKeyGetter(name)
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def getPropertyKeys(self):
        self._check_if_connection_is_established_()

        getter = PropertyKeyGetter()
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def getVertexLabel(self, label):
        self._check_if_connection_is_established_()

        getter = VertexLabelGetter(label)
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def getVertexLabels(self):
        self._check_if_connection_is_established_()

        getter = VertexLabelGetter()
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def getEdgeLabel(self, label):
        self._check_if_connection_is_established_()

        getter = EdgeLabelGetter(label)
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def getEdgeLabels(self):
        self._check_if_connection_is_established_()

        getter = EdgeLabelGetter()
        getter.set_channel(self.CHANNEL)
        getter.set_graph(self.GRAPH)

        return getter.get()

    def buildCompositeIndex(self, name):
        self._check_if_connection_is_established_()

        maker = CompositeIndexMaker(name)
        maker.set_graph(self.GRAPH)
        maker.set_channel(self.CHANNEL)

        return maker

    def getVertexCompositeIndex(self, name):
        self._check_if_connection_is_established_()

        getter = CompositeIndexGetter("VertexLabel")
        getter.set_graph(self.GRAPH)
        getter.set_channel(self.CHANNEL)
        getter.restrict_name(name)

        return getter.get()

    def getEdgeCompositeIndex(self, name):
        self._check_if_connection_is_established_()

        getter = CompositeIndexGetter("EdgeLabel")
        getter.set_graph(self.GRAPH)
        getter.set_channel(self.CHANNEL)
        getter.restrict_name(name)

        return getter.get()

    def enableCompositeIndex(self, index):
        """

        Args:
            index (CompositeIndex):

        Returns:

        """
        enabler = CompositeIndexEnabler()
        enabler.set_index(index)
        enabler.set_channel(self.CHANNEL)
        enabler.set_graph(self.GRAPH)

        return enabler

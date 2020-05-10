import grpc

from janusgraph.management.builder.vertex_label_maker import VertexLabelMaker
from janusgraph.management.builder.edge_label_maker import EdgeLabelMaker


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

    def _create_channel_(self):
        self.CHANNEL = grpc.insecure_channel(f'{self.HOST}:{self.PORT}')
        return self

    def _check_if_connection_is_established_(self):
        if self.HOST is None or self.PORT is None or self.GRAPH is None or self.CHANNEL is None:
            raise ValueError("Please call connect() before calling any make() instances")

    def makeVertexLabel(self, label):
        self._check_if_connection_is_established_()

        maker = VertexLabelMaker(label)
        maker.set_channel(self.CHANNEL)

        return maker

    def makeEdgelabel(self, label):
        self._check_if_connection_is_established_()

        maker = EdgeLabelMaker(label)
        maker.set_channel(self.CHANNEL)

        return maker

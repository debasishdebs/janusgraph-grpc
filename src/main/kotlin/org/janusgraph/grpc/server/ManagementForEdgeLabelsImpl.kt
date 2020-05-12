package org.janusgraph.grpc.server

import io.grpc.stub.StreamObserver
import org.janusgraph.grpc.*

class ManagementForEdgeLabelsImpl(
    private val managementServer: IManagementForEdgeLabels,
    private val contextManager: ContextManager
) :
    ManagementForEdgeLabelsGrpc.ManagementForEdgeLabelsImplBase() {

    override fun getEdgeLabelsByName(
        request: GetEdgeLabelsByNameRequest?,
        responseObserver: StreamObserver<EdgeLabel>?
    ) {
        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.name == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        val labels = managementServer.getEdgeLabelsByName(management, request.name)
        labels.forEach { responseObserver?.onNext(it) }
        responseObserver?.onCompleted()
    }

    override fun getEdgeLabels(request: GetEdgeLabelsRequest?, responseObserver: StreamObserver<EdgeLabel>?) {
        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        val labels = managementServer.getEdgeLabels(management)
        labels.forEach { responseObserver?.onNext(it) }
        responseObserver?.onCompleted()
    }

    override fun ensureEdgeLabel(request: EnsureEdgeLabelRequest?, responseObserver: StreamObserver<EdgeLabel>?) {
        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.label == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        val edgeLabel = managementServer.ensureEdgeLabel(management, request.label)
        responseObserver?.onNext(edgeLabel)
        responseObserver?.onCompleted()
    }

    override fun ensureCompositeIndexByEdgeLabel(
        request: EnsureCompositeIndexByEdgeLabelRequest?,
        responseObserver: StreamObserver<CompositeEdgeIndex>?
    ) {
        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.edgeLabel == null) {
            responseObserver?.onError(Throwable("Not set edgeLabel"))
            return
        }
        if (request.index == null) {
            responseObserver?.onError(Throwable("Not set index"))
            return
        }
        val index = managementServer.ensureCompositeIndexByEdgeLabel(management, request.edgeLabel, request.index)
        responseObserver?.onNext(index)
        responseObserver?.onCompleted()
    }

    override fun enableEdgeCompositeIndex(
        request: EnableEdgeCompositeIndexByNameRequest,
        responseObserver: StreamObserver<CompositeEdgeIndex>
    ) {
        val graph = contextManager.getGraph(request.context)
        if (graph == null) {
            responseObserver.onError(Throwable("Incorrect context"))
            return
        }

        val index = managementServer.enableEdgeCompositeIndex(graph, request.index)
        responseObserver.onNext(index)
        responseObserver.onCompleted()
    }

    override fun ensureCompositeIndexForEdge(
        request: EnsureCompositeIndexForEdgesRequest?,
        responseObserver: StreamObserver<CompositeEdgeIndex>?
    ) {
        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.index == null) {
            responseObserver?.onError(Throwable("Not set index"))
            return
        }
        val index = managementServer.ensureCompositeIndexForEdge(management, request.index)
        responseObserver?.onNext(index)
        responseObserver?.onCompleted()
    }

    override fun getCompositeIndicesByEdgeLabel(
        request: GetCompositeIndicesByEdgeLabelRequest?,
        responseObserver: StreamObserver<CompositeEdgeIndex>?
    ) {
        val graph = contextManager.getGraph(request?.context)
        if (graph == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.edgeLabel == null) {
            responseObserver?.onError(Throwable("Not set edgeLabel"))
            return
        }
        val indices = managementServer.getCompositeIndicesByEdgeLabel(graph, request.edgeLabel)
        indices.forEach { responseObserver?.onNext(it) }
        responseObserver?.onCompleted()
    }

    override fun getEdgeCompositeIndexByName(
        request: GetCompositeIndicesByNameRequest?,
        responseObserver: StreamObserver<CompositeEdgeIndex>?
    ) {
        val graph = contextManager.getGraph(request?.context)
        if (graph == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.indexName == null) {
            responseObserver?.onError(Throwable("indexName not set for querying CompositeIndex"))
            return
        }
        val index = managementServer.getEdgeCompositeIndexByName(graph, request.indexName)
        responseObserver?.onNext(index)
        responseObserver?.onCompleted()
    }

    override fun getCompositeIndicesForEdge(
        request: GetCompositeIndicesForEdgeRequest,
        responseObserver: StreamObserver<CompositeEdgeIndex>?
    ) {
        val graph = contextManager.getGraph(request.context)
        if (graph == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        val indices = managementServer.getCompositeIndicesForEdge(graph)
        indices.forEach { responseObserver?.onNext(it) }
        responseObserver?.onCompleted()
    }
}

package org.janusgraph.grpc.server

import io.grpc.stub.StreamObserver
import org.janusgraph.grpc.*


class ManagementForPropertyKeysImpl (
    private val managementServer: IManagementForPropertyKeys,
    private val contextManager: ContextManager
) : ManagementForPropertyKeysGrpc.ManagementForPropertyKeysImplBase() {

    override fun getPropertyKeys(
        request: GetPropertyKeysRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

    }

    override fun getPropertyKeyByName(
        request: GetPropertyKeyByNameRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

    }

    override fun ensurePropertyKeyForLabel(
        request: EnsurePropertyKeyForLabelRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

    }

    override fun ensurePropertyKey(
        request: EnsurePropertyKeyRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

    }

    override fun ensurePropertyKeyForVertexLabel(
        request: EnsurePropertyKeyForVertexLabelRequest?,
        responseObserver: StreamObserver<VertexProperty>?) {

    }

    override fun ensurePropertyKeyForEdgeLabel(
        request: EnsurePropertyKeyForEdgeLabelRequest?,
        responseObserver: StreamObserver<EdgeProperty>?) {

    }
}

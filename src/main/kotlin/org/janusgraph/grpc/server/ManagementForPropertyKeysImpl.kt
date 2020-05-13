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

        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        val propertyKeys = managementServer.getPropertyKeys(management)
        propertyKeys.forEach { responseObserver?.onNext(it) }
        responseObserver?.onCompleted()
    }

    override fun getPropertyKeyByName(
        request: GetPropertyKeyByNameRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.name == null) {
            responseObserver?.onError(Throwable("Not set name"))
            return
        }
        val propertyKey = managementServer.getPropertyKeyByName(management, request.name)
        responseObserver?.onNext(propertyKey)
        responseObserver?.onCompleted()
    }

    override fun ensurePropertyKey(
        request: EnsurePropertyKeyRequest?,
        responseObserver: StreamObserver<PropertyKey>?) {

        val management = contextManager.getManagement(request?.context)
        if (management == null) {
            responseObserver?.onError(Throwable("Incorrect context"))
            return
        }
        if (request?.property == null) {
            responseObserver?.onError(Throwable("Not set property"))
            return
        }
        val propertyKey = managementServer.ensurePropertyKey(management, request.property)
        responseObserver?.onNext(propertyKey)
        responseObserver?.onCompleted()
    }
}

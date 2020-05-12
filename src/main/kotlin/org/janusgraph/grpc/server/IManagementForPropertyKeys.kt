package org.janusgraph.grpc.server

import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.grpc.*

interface IManagementForPropertyKeys {
    fun getPropertyKeys (management: JanusGraphManagement): List<PropertyKey>
    fun getPropertyKeyByName (management: JanusGraphManagement, name: String): PropertyKey

    fun ensurePropertyKeyForLabel (management: JanusGraphManagement, requestPropertyKey: PropertyKey, label: String): PropertyKey
    fun ensurePropertyKey (management: JanusGraphManagement, requestPropertyKey: PropertyKey): PropertyKey
    fun ensurePropertyKeyForVertexLabel (management: JanusGraphManagement, requestPropertyKey: VertexProperty, label: VertexLabel): VertexProperty
    fun ensurePropertyKeyForEdgeLabel (management: JanusGraphManagement, requestPropertyKey: EdgeProperty, label: EdgeLabel): EdgeProperty
}

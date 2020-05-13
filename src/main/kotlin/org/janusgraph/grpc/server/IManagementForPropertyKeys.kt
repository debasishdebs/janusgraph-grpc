package org.janusgraph.grpc.server

import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.grpc.*

interface IManagementForPropertyKeys {
    fun getPropertyKeys (management: JanusGraphManagement): List<PropertyKey>
    fun getPropertyKeyByName (management: JanusGraphManagement, name: String): PropertyKey

    fun ensurePropertyKey (management: JanusGraphManagement, requestPropertyKey: PropertyKey): PropertyKey
}

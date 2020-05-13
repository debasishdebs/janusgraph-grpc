package org.janusgraph.grpc.server

import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.grpc.*

class ManagementForPropertyKeys: IManagementForPropertyKeys {

    override fun getPropertyKeys(management: JanusGraphManagement): List<PropertyKey> {
        return management.getRelationTypes(org.janusgraph.core.PropertyKey::class.java)
            ?.map { createPropertyKeysProto(it) } ?: return emptyList()
    }

    override fun getPropertyKeyByName(management: JanusGraphManagement, name: String): PropertyKey {
        return createPropertyKeysProto(management.getPropertyKey(name))
    }

    private fun getOrCreateProperty(
        management: JanusGraphManagement,
        property: PropertyKey
    ): org.janusgraph.core.PropertyKey {
        val propertyKey =
            management.getPropertyKey(property.name) ?: management
                .makePropertyKey(property.name)
                .dataType(convertDataTypeToJavaClass(property.dataType))
                .cardinality(convertCardinalityToJavaClass(property.cardinality))
                .make()

        return propertyKey
    }

    override fun ensurePropertyKey(management: JanusGraphManagement, requestPropertyKey: PropertyKey): PropertyKey {
        val property = getOrCreateProperty(management, requestPropertyKey)
        val response = createPropertyKeysProto(property)
        management.commit()
        return response
    }
}

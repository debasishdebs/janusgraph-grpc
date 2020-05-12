package org.janusgraph.grpc.server

import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.grpc.*
import java.lang.IllegalArgumentException
import java.lang.NullPointerException

class ManagementForPropertyKeys: IManagementForPropertyKeys {

    override fun getPropertyKeys(management: JanusGraphManagement): List<PropertyKey> {
        return management.getRelationTypes(org.janusgraph.core.PropertyKey::class.java)
            ?.map { createPropertyKeysProto(it) } ?: return emptyList()
    }

    override fun getPropertyKeyByName(management: JanusGraphManagement, name: String): PropertyKey {
        return createPropertyKeysProto(management.getPropertyKey(name))
    }

    override fun ensurePropertyKeyForLabel(
        management: JanusGraphManagement,
        requestPropertyKey: PropertyKey,
        label: String
    ): PropertyKey {

        val response: PropertyKey

        val elementLabel = management.getVertexLabel(label)
        response = if (elementLabel != null) {
            val property = getOrCreateVertexProperty(management, elementLabel, requestPropertyKey)
            createPropertyKeysProto(property)
        } else {
            val edgeLabel = management.getEdgeLabel(label)
                ?: throw IllegalAccessException("no vertex or edge found with label=${label}")

            val property = getOrCreateEdgeProperty(management, edgeLabel, requestPropertyKey)
            createPropertyKeysProto(property)
        }

        management.commit()
        println("====")
        println(elementLabel.mappedProperties())
        return response
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

    private fun getOrCreateVertexProperty(
        management: JanusGraphManagement,
        label: org.janusgraph.core.VertexLabel,
        property: PropertyKey
    ): org.janusgraph.core.PropertyKey {
        val propertyKey =
            management.getPropertyKey(property.name) ?: management
                .makePropertyKey(property.name)
                .dataType(convertDataTypeToJavaClass(property.dataType))
                .cardinality(convertCardinalityToJavaClass(property.cardinality))
                .make()

        val connections = label.mappedProperties()
        println(propertyKey.name())
        println(connections)
        println(connections.contains(propertyKey))
        if (!connections.contains(propertyKey)) {
            println("So I'm adding maps")
            management.addProperties(label, propertyKey)
        }
        return propertyKey
    }

    private fun getOrCreateEdgeProperty(
        management: JanusGraphManagement,
        label: org.janusgraph.core.EdgeLabel,
        property: PropertyKey
    ): org.janusgraph.core.PropertyKey {
        val propertyKey =
            management.getPropertyKey(property.name) ?: management
                .makePropertyKey(property.name)
                .dataType(convertDataTypeToJavaClass(property.dataType))
                .make()

        val connections = label.mappedProperties()
        if (!connections.contains(propertyKey)) {
            management.addProperties(label, propertyKey)
        }
        return propertyKey
    }


    override fun ensurePropertyKey(management: JanusGraphManagement, requestPropertyKey: PropertyKey): PropertyKey {
        val property = getOrCreateProperty(management, requestPropertyKey)
        val response = createPropertyKeysProto(property)
        management.commit()
        return response
    }

    override fun ensurePropertyKeyForVertexLabel(
        management: JanusGraphManagement,
        requestPropertyKey: VertexProperty,
        label: VertexLabel
    ): VertexProperty {

        val elementLabel = management.getVertexLabel(label.name) ?: throw NullPointerException("VertexLabel doesn't exist")

        val property = getOrCreateVertexProperty(management, elementLabel, convertVertexPropertyToPropertyKey(requestPropertyKey))
        val response = createVertexPropertyKeysProto(property)
        management.commit()
        return response
    }

    override fun ensurePropertyKeyForEdgeLabel(
        management: JanusGraphManagement,
        requestPropertyKey: EdgeProperty,
        label: EdgeLabel
    ): EdgeProperty {

        val elementLabel = management.getEdgeLabel(label.name) ?: throw NullPointerException("EdgeLabel doesn't exist")

        val property = getOrCreateEdgeProperty(management, elementLabel, convertEdgePropertyToPropertyKey(requestPropertyKey))
        val response = createEdgePropertyProto(property)
        management.commit()
        return response
    }

}

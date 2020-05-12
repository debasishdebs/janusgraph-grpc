package org.janusgraph.grpc.server

import com.google.protobuf.Field
import com.google.protobuf.Int64Value
import com.sun.org.apache.xpath.internal.operations.Bool
import org.apache.tinkerpop.gremlin.structure.Element
import org.janusgraph.core.Cardinality
import org.janusgraph.core.JanusGraphVertex
import org.janusgraph.core.Multiplicity
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.attribute.Geoshape
import org.janusgraph.core.schema.SchemaStatus
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory
import org.janusgraph.graphdb.query.QueryUtil
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.janusgraph.graphdb.types.IndexType
import org.janusgraph.graphdb.types.system.BaseKey
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex
import org.janusgraph.grpc.*
import java.util.*


internal fun convertDataTypeToJavaClass(dataType: PropertyDataType): Class<*> =
    when (dataType) {
        PropertyDataType.String -> java.lang.String::class.java
        PropertyDataType.Character -> java.lang.Character::class.java
        PropertyDataType.Boolean -> java.lang.Boolean::class.java
        PropertyDataType.Int8 -> java.lang.Byte::class.java
        PropertyDataType.Int16 -> java.lang.Short::class.java
        PropertyDataType.Int32 -> java.lang.Integer::class.java
        PropertyDataType.Int64 -> java.lang.Long::class.java
        PropertyDataType.Float32 -> java.lang.Float::class.java
        PropertyDataType.Float64 -> java.lang.Double::class.java
        PropertyDataType.Date -> Date::class.java
        PropertyDataType.JavaObject -> Any::class.java
        PropertyDataType.GeoShape -> Geoshape::class.java
        PropertyDataType.Uuid -> UUID::class.java
        PropertyDataType.UNRECOGNIZED -> TODO()
    }

internal fun convertJavaClassToDataType(dataType: Class<*>): PropertyDataType? =
    when (dataType) {
        java.lang.String::class.java -> PropertyDataType.String
        java.lang.Character::class.java -> PropertyDataType.Character
        java.lang.Boolean::class.java -> PropertyDataType.Boolean
        java.lang.Byte::class.java -> PropertyDataType.Int8
        java.lang.Short::class.java -> PropertyDataType.Int16
        java.lang.Integer::class.java -> PropertyDataType.Int32
        java.lang.Long::class.java -> PropertyDataType.Int64
        java.lang.Float::class.java -> PropertyDataType.Float32
        java.lang.Double::class.java -> PropertyDataType.Float64
        Date::class.java -> PropertyDataType.Date
        Any::class.java -> PropertyDataType.JavaObject
        Geoshape::class.java -> PropertyDataType.GeoShape
        UUID::class.java -> PropertyDataType.Uuid
        else -> PropertyDataType.UNRECOGNIZED
    }

internal fun convertCardinalityToJavaClass(cardinality: org.janusgraph.grpc.Cardinality): Cardinality =
    when (cardinality) {
        org.janusgraph.grpc.Cardinality.Single -> Cardinality.SINGLE
        org.janusgraph.grpc.Cardinality.List -> Cardinality.LIST
        org.janusgraph.grpc.Cardinality.Set -> Cardinality.SET
        org.janusgraph.grpc.Cardinality.UNRECOGNIZED -> TODO()
    }

internal fun convertJavaClassToCardinality(cardinality: Cardinality): org.janusgraph.grpc.Cardinality? {
    return when (cardinality) {
        Cardinality.SINGLE -> org.janusgraph.grpc.Cardinality.Single
        Cardinality.LIST -> org.janusgraph.grpc.Cardinality.List
        Cardinality.SET -> org.janusgraph.grpc.Cardinality.Set
    }
}


internal fun convertVertexPropertyToPropertyKey(propertyKey: VertexProperty): org.janusgraph.grpc.PropertyKey {
    return org.janusgraph.grpc.PropertyKey.newBuilder()
        .setId(propertyKey.id)
        .setName(propertyKey.name)
        .setCardinality(propertyKey.cardinality)
        .setDataType(propertyKey.dataType)
        .build()
}

internal fun convertSchemaStatusToString(status: SchemaStatus): String {
    return when (status) {
        SchemaStatus.ENABLED -> "ENABLED"
        SchemaStatus.DISABLED -> "DISABLED"
        SchemaStatus.INSTALLED -> "INSTALLED"
        SchemaStatus.REGISTERED -> "REGISTERED"
    }
}

internal fun convertEdgePropertyToPropertyKey(propertyKey: EdgeProperty): org.janusgraph.grpc.PropertyKey {
    return org.janusgraph.grpc.PropertyKey.newBuilder()
        .setId(propertyKey.id)
        .setName(propertyKey.name)
        .setCardinality(null)
        .setDataType(propertyKey.dataType)
        .build()
}

internal fun convertMultiplicityToJavaClass(multiplicity: EdgeLabel.Multiplicity): Multiplicity =
    when (multiplicity) {
        EdgeLabel.Multiplicity.Simple -> Multiplicity.SIMPLE
        EdgeLabel.Multiplicity.One2One -> Multiplicity.ONE2ONE
        EdgeLabel.Multiplicity.One2Many -> Multiplicity.ONE2MANY
        EdgeLabel.Multiplicity.Multi -> Multiplicity.MULTI
        EdgeLabel.Multiplicity.Many2One -> Multiplicity.MANY2ONE
        EdgeLabel.Multiplicity.UNRECOGNIZED -> TODO()
    }

internal fun convertDirectedToBool(directed: EdgeLabel.Directed): Boolean =
    when (directed) {
        EdgeLabel.Directed.undirected_edge -> false
        EdgeLabel.Directed.directed_edge -> true
        EdgeLabel.Directed.UNRECOGNIZED -> TODO()
    }

internal fun convertBooleanDirectedToDirected(directedStatus: Boolean): EdgeLabel.Directed {
    return when (directedStatus) {
        true -> EdgeLabel.Directed.directed_edge
        false -> EdgeLabel.Directed.undirected_edge
    }
}

internal fun convertJavaClassMultiplicity(multiplicity: Multiplicity): EdgeLabel.Multiplicity? {
    return when (multiplicity) {
        Multiplicity.MANY2ONE -> EdgeLabel.Multiplicity.Many2One
        Multiplicity.MULTI -> EdgeLabel.Multiplicity.Multi
        Multiplicity.ONE2MANY -> EdgeLabel.Multiplicity.One2Many
        Multiplicity.ONE2ONE -> EdgeLabel.Multiplicity.One2One
        Multiplicity.SIMPLE -> EdgeLabel.Multiplicity.Simple
    }
}

internal fun createVertexLabelProto(vertexLabel: org.janusgraph.core.VertexLabel, properties: List<PropertyKey>) =
    VertexLabel.newBuilder()
        .setId(Int64Value.of(vertexLabel.longId()))
        .setName(vertexLabel.name())
        .addAllProperties(properties.map { createVertexPropertyKeysProto(it) })
        .setPartitioned(vertexLabel.isPartitioned)
        .setReadOnly(vertexLabel.isStatic)
        .build()

internal fun createEdgePropertyProto(property: PropertyKey): EdgeProperty =
    EdgeProperty.newBuilder()
        .setName(property.name())
        .setDataType(convertJavaClassToDataType(property.dataType()))
        .build()

internal fun createEdgeLabelProto(edgeLabel: org.janusgraph.core.EdgeLabel, properties: List<PropertyKey>) =
    EdgeLabel.newBuilder()
        .setId(Int64Value.of(edgeLabel.longId()))
        .setName(edgeLabel.name())
        .addAllProperties(properties.map { createEdgePropertyProto(it) })
        .setMultiplicity(convertJavaClassMultiplicity(edgeLabel.multiplicity()))
        .setDirected(convertBooleanDirectedToDirected(edgeLabel.isDirected))
        .build()

internal fun createPropertyKeysProto(propertyKey: PropertyKey): org.janusgraph.grpc.PropertyKey =
    org.janusgraph.grpc.PropertyKey.newBuilder()
        .setId(Int64Value.of(propertyKey.longId()))
        .setName(propertyKey.name())
        .setDataType(convertJavaClassToDataType(propertyKey.dataType()))
        .setCardinality(convertJavaClassToCardinality(propertyKey.cardinality()))
        .build()

internal fun createVertexPropertyKeysProto(propertyKey: PropertyKey): VertexProperty =
    VertexProperty.newBuilder()
        .setId(Int64Value.of(propertyKey.longId()))
        .setName(propertyKey.name())
        .setDataType(convertJavaClassToDataType(propertyKey.dataType()))
        .setCardinality(convertJavaClassToCardinality(propertyKey.cardinality()))
        .build()

internal fun getGraphIndices(tx: StandardJanusGraphTx, clazz: Class<out Element>): List<IndexType> =
    QueryUtil.getVertices(tx, BaseKey.SchemaCategory, JanusGraphSchemaCategory.GRAPHINDEX)
        .map { janusGraphVertex: JanusGraphVertex ->
            assert(janusGraphVertex is JanusGraphSchemaVertex)
            (janusGraphVertex as JanusGraphSchemaVertex).asIndexType()
        }
        .filter { indexType: IndexType ->
            indexType.element.subsumedBy(clazz)
        }

internal fun getVertexLabels(tx: StandardJanusGraphTx): List<org.janusgraph.core.VertexLabel> = QueryUtil
    .getVertices(tx, BaseKey.SchemaCategory, JanusGraphSchemaCategory.VERTEXLABEL)
    .mapNotNull { it as? org.janusgraph.core.VertexLabel }
    .toList()

internal fun getEdgeLabels(tx: StandardJanusGraphTx): List<org.janusgraph.core.EdgeLabel> = QueryUtil
    .getVertices(tx, BaseKey.SchemaCategory, JanusGraphSchemaCategory.EDGELABEL)
    .mapNotNull { it as? org.janusgraph.core.EdgeLabel }
    .toList()

package org.janusgraph.grpc.server

import org.apache.tinkerpop.gremlin.structure.Vertex
import org.janusgraph.core.Cardinality
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.core.schema.SchemaAction
import org.janusgraph.core.schema.SchemaStatus
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.janusgraph.graphdb.types.CompositeIndexType
import org.janusgraph.graphdb.types.MixedIndexType
import org.janusgraph.grpc.*

class ManagementForVertexLabels : IManagementForVertexLabels {

    override fun getVertexLabelsByName(management: JanusGraphManagement, name: String): List<VertexLabel> {
        val vertexLabel = management.getVertexLabel(name) ?: return emptyList()

        return listOf(
            createVertexLabelProto(
                vertexLabel,
                vertexLabel.mappedProperties().toList()
            )
        )
    }

    override fun getVertexLabels(management: JanusGraphManagement): List<VertexLabel> {
        return management.vertexLabels?.map {
            createVertexLabelProto(
                it,
                it.mappedProperties().toList()
            )
        } ?: return emptyList()
    }

    private fun getVertexLabel(
        management: JanusGraphManagement,
        label: VertexLabel
    ): org.janusgraph.core.VertexLabel? =
        if (label.hasId()) {
            management.vertexLabels.first { it.longId() == label.id.value }
                ?: throw IllegalArgumentException("No vertexLabel found with id")
        } else {
            print("I'm retrieving vertex label with name " + label.name)
            management.getVertexLabel(label.name)
        }

    private fun getOrCreateVertexProperty(
        management: JanusGraphManagement,
        label: org.janusgraph.core.VertexLabel,
        property: VertexProperty
    ): PropertyKey {
        val propertyKey =
            management.getPropertyKey(property.name) ?: management
                .makePropertyKey(property.name)
                .dataType(convertDataTypeToJavaClass(property.dataType))
                .cardinality(convertCardinalityToJavaClass(property.cardinality))
                .make()
        val connections = label.mappedProperties()
        if (!connections.contains(propertyKey)) {
            management.addProperties(label, propertyKey)
        }
        return propertyKey
    }

    override fun ensureVertexLabel(management: JanusGraphManagement, requestLabel: VertexLabel): VertexLabel? {
        val label = getVertexLabel(management, requestLabel)
        val name = requestLabel.name ?: throw NullPointerException("name should not be null")

        println("Label is $label and requestLabel is $requestLabel")

        val vertexLabel = when {
            label?.name() == name -> label
            label != null -> {
                management.changeName(label, name)
                label
            }
            else -> {
                val vertexLabelMaker = management.makeVertexLabel(name)
                if (requestLabel.readOnly)
                    vertexLabelMaker.setStatic()
                if (requestLabel.partitioned)
                    vertexLabelMaker.partition()
                val vertexLabel = vertexLabelMaker.make()

                println(requestLabel)
                println("Label is $name and static is ${requestLabel.readOnly} and partitioned is ${requestLabel.partitioned}")

                vertexLabel
            }
        }

        println("The result object is $vertexLabel")
        println("Name: ${vertexLabel.name()} readOnly: ${vertexLabel.isStatic} and partitioned: ${vertexLabel.isPartitioned}")

        val properties = requestLabel.propertiesList.map { getOrCreateVertexProperty(management, vertexLabel, it) }
        val response = createVertexLabelProto(vertexLabel, properties)
        management.commit()
        return response
    }

    override fun ensureCompositeIndexByVertexLabel(
        management: JanusGraphManagement,
        requestLabel: VertexLabel,
        requestIndex: CompositeVertexIndex
    ): CompositeVertexIndex? {
        val label = getVertexLabel(management, requestLabel) ?: throw NullPointerException("vertex should exists")

        val keys = requestIndex.propertiesList.map { management.getPropertyKey(it.name) }
        val builder = management.buildIndex(requestIndex.name, Vertex::class.java)
            .indexOnly(label)

        if (requestIndex.unique)
            builder.unique()

        keys.forEach { builder.addKey(it) }

        val graphIndex = builder.buildCompositeIndex()
        val properties = graphIndex.fieldKeys.map { createVertexPropertyKeysProto(it) }

        val compositeVertexIndex = CompositeVertexIndex.newBuilder()
            .setName(graphIndex.name())
            .addAllProperties(properties)
            .setUnique(graphIndex.isUnique)
            .setStatus(convertSchemaStatusToString(graphIndex.getIndexStatus(keys[0])))
            .setLabel(requestLabel.name)
            .build()
        management.commit()
        return compositeVertexIndex
    }

    private fun getVertexLabelTx(
        tx: StandardJanusGraphTx,
        label: VertexLabel
    ): org.janusgraph.core.VertexLabel? =
        if (label.hasId()) {
            getVertexLabels(tx).firstOrNull { it.longId() == label.id.value }
                ?: throw IllegalArgumentException("No vertexLabel found with id")
        } else {
            tx.getVertexLabel(label.name)
        }

    override fun getCompositeIndicesByVertexLabel(
        graph: StandardJanusGraph,
        requestLabel: VertexLabel
    ): List<CompositeVertexIndex> {
        val tx = graph.buildTransaction().disableBatchLoading().start() as StandardJanusGraphTx
        val label = getVertexLabelTx(tx, requestLabel)
        val graphIndexes = getGraphIndices(tx, Vertex::class.java)
        val indices = graphIndexes
            .filterIsInstance<CompositeIndexType>()
            .filter { it.schemaTypeConstraint == label }
            .map {
                CompositeVertexIndex.newBuilder()
                    .setName(it.name)
                    .addAllProperties(it.fieldKeys.map { property -> createVertexPropertyKeysProto(property.fieldKey) })
                    .setUnique(it.cardinality == Cardinality.SINGLE)
                    .setStatus(convertSchemaStatusToString(it.status))
                    .setLabel(it.schemaTypeConstraint.toString())
                    .build()
            }
        tx.rollback()
        return indices
    }

    override fun getVertexCompositeIndexByName(
        graph: StandardJanusGraph,
        indexName: String
    ): CompositeVertexIndex {
        val tx = graph.buildTransaction().disableBatchLoading().start() as StandardJanusGraphTx
        val graphIndexes = getGraphIndices(tx, Vertex::class.java)
        val indices = graphIndexes
            .filterIsInstance<CompositeIndexType>()
            .filter { it.name == indexName }
            .map {

                val label: String
                if (it.schemaTypeConstraint == null)
                    label = "ALL"
                else
                    label = it.schemaTypeConstraint.toString()

                println(it.schemaTypeConstraint)
                CompositeVertexIndex.newBuilder()
                    .setName(it.name)
                    .addAllProperties(it.fieldKeys.map { property -> createVertexPropertyKeysProto(property.fieldKey) })
                    .setUnique(it.cardinality == Cardinality.SINGLE)
                    .setStatus(convertSchemaStatusToString(it.status))
                    .setLabel(label)
                    .build()
            }
        tx.rollback()
        return indices.first()
    }

    override fun getCompositeIndicesForVertex(
        graph: StandardJanusGraph
    ): List<CompositeVertexIndex> {
        val tx = graph.buildTransaction().disableBatchLoading().start() as StandardJanusGraphTx
        val graphIndexes = getGraphIndices(tx, Vertex::class.java)
        val indices = graphIndexes
            .filterIsInstance<CompositeIndexType>()
            .map {

                val label: String
                if (it.schemaTypeConstraint == null)
                    label = "ALL"
                else
                    label = it.schemaTypeConstraint.toString()

                CompositeVertexIndex.newBuilder()
                    .setName(it.name)
                    .addAllProperties(it.fieldKeys.map { property -> createVertexPropertyKeysProto(property.fieldKey) })
                    .setUnique(it.cardinality == Cardinality.SINGLE)
                    .setStatus(convertSchemaStatusToString(it.status))
                    .setLabel(label)
                    .build()
            }
        tx.rollback()
        return indices
    }

    override fun enableCompositeIndexByName(graph: StandardJanusGraph, indexName: String): CompositeVertexIndex {
        ManagementSystem.awaitGraphIndexStatus(graph, indexName).call()

        val management = graph.openManagement()
        val index = management.getGraphIndex(indexName)
        if (index.getIndexStatus(index.fieldKeys[0]) == SchemaStatus.REGISTERED) {
            management.updateIndex(index, SchemaAction.ENABLE_INDEX)
            println("Changed from REGISTERED to ENABLED")
        } else if (index.getIndexStatus(index.fieldKeys[0]) == SchemaStatus.INSTALLED)
            throw IllegalAccessError("Exception in converting Index from INSTALLED to REGISTERED/ENABLED")
        else if (index.getIndexStatus(index.fieldKeys[0]) == SchemaStatus.DISABLED)
            throw IllegalAccessException("Index can't be created on Index with status DISABLED")

        val compositeIndex = CompositeVertexIndex.newBuilder()
            .setName(index.name())
            .addAllProperties(index.fieldKeys.map { createVertexPropertyKeysProto(it) })
            .setStatus(convertSchemaStatusToString(index.getIndexStatus(index.fieldKeys[0])))
            .build()
        management.commit()

        return compositeIndex
    }

    override fun ensureCompositeIndexForVertex(
        management: JanusGraphManagement,
        requestIndex: CompositeVertexIndex
    ): CompositeVertexIndex? {

        val keys = requestIndex.propertiesList.map { management.getPropertyKey(it.name) }
        val builder = management.buildIndex(requestIndex.name, Vertex::class.java)

        if (requestIndex.unique)
            builder.unique()

        keys.forEach { builder.addKey(it) }

        val graphIndex = builder.buildCompositeIndex()
        val properties = graphIndex.fieldKeys.map { createVertexPropertyKeysProto(it) }

        val compositeVertexIndex = CompositeVertexIndex.newBuilder()
            .setName(graphIndex.name())
            .addAllProperties(properties)
            .setUnique(graphIndex.isUnique)
            .setStatus(convertSchemaStatusToString(graphIndex.getIndexStatus(keys[0])))
            .setLabel("ALL")
            .build()
        management.commit()
        return compositeVertexIndex
    }

    override fun ensureMixedIndexByVertexLabel(
        management: JanusGraphManagement,
        requestLabel: VertexLabel,
        requestIndex: MixedVertexIndex
    ): MixedVertexIndex? {
        val label = getVertexLabel(management, requestLabel) ?: throw NullPointerException("vertex should exists")

        val keys = requestIndex.propertiesList.map { management.getPropertyKey(it.name) }
        val builder = management.buildIndex(requestIndex.name, Vertex::class.java)
            .indexOnly(label)

        keys.forEach { builder.addKey(it) }

        val graphIndex = builder.buildMixedIndex(requestIndex.backend)
        val properties = graphIndex.fieldKeys.map { createVertexPropertyKeysProto(it) }

        val compositeVertexIndex = MixedVertexIndex.newBuilder()
            .setName(graphIndex.name())
            .addAllProperties(properties)
            .setBackend(graphIndex.backingIndex)
            .build()
        management.commit()
        return compositeVertexIndex
    }

    override fun getMixedIndicesByVertexLabel(
        graph: StandardJanusGraph,
        requestLabel: VertexLabel
    ): List<MixedVertexIndex> {
        val tx = graph.buildTransaction().disableBatchLoading().start() as StandardJanusGraphTx
        val label = getVertexLabelTx(tx, requestLabel)
        val graphIndexes = getGraphIndices(tx, Vertex::class.java)
        val indices = graphIndexes
            .filterIsInstance<MixedIndexType>()
            .filter { it.schemaTypeConstraint == label }
            .map {
                MixedVertexIndex.newBuilder()
                    .setName(it.name)
                    .setBackend(it.backingIndexName)
                    .addAllProperties(it.fieldKeys.map { property -> createVertexPropertyKeysProto(property.fieldKey) })
                    .build()
            }
        tx.rollback()
        return indices
    }
}

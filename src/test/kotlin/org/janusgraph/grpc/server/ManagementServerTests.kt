package org.janusgraph.grpc.server

import com.google.protobuf.Int64Value
import org.janusgraph.grpc.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource


class ManagementServerTests {

    private fun createContextManager(): ContextManager =
        ContextManagerImpl(
            hashMapOf(
                "first" to JanusGraphTestUtils.getJanusGraph(),
                "second" to JanusGraphTestUtils.getJanusGraph()
            )
        )

    private fun createContext(graphName: String = "first") =
        JanusGraphContext.newBuilder().setGraphName(graphName).build()

    private fun createManagementServer(): ManagementServer =
        ManagementServer(createContextManager())

    private fun ensureBasicVertexLabelRequest(
        graphName: String = "first",
        name: String = "test",
        vertexId: Long? = null,
        properties: List<VertexProperty> = emptyList()
    ): EnsureVertexLabelRequest {
        val vertexLabelBuilder = VertexLabel.newBuilder().setName(name).addAllProperties(properties)
        if (vertexId != null) {
            vertexLabelBuilder.id = Int64Value.of(vertexId)
        }
        return EnsureVertexLabelRequest.newBuilder()
            .setContext(createContext(graphName))
            .setLabel(vertexLabelBuilder.build())
            .build()
    }

    private fun ensureBasicEdgeLabelRequest(
        graphName: String = "first",
        name: String = "test",
        edgeId: Long? = null,
        properties: List<EdgeProperty> = emptyList()
    ): EnsureEdgeLabelRequest {
        val edgeLabelBuilder = EdgeLabel.newBuilder().setName(name).addAllProperties(properties)
        if (edgeId != null) {
            edgeLabelBuilder.id = Int64Value.of(edgeId)
        }
        return EnsureEdgeLabelRequest.newBuilder()
            .setContext(createContext(graphName))
            .setLabel(edgeLabelBuilder.build())
            .build()
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureVertexLabel create basic vertexLabel`(graphName: String) {
        val managementServer = createManagementServer()
        val request = ensureBasicVertexLabelRequest(graphName)

        val vertexLabel = managementServer.ensureVertexLabel(request)

        assertEquals("test", vertexLabel?.name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureVertexLabel can run multiple times`(graphName: String) {
        val managementServer = createManagementServer()
        val request = ensureBasicVertexLabelRequest(graphName)

        managementServer.ensureVertexLabel(request)
        managementServer.ensureVertexLabel(request)
        val vertexLabel = managementServer.ensureVertexLabel(request)

        assertEquals("test", vertexLabel?.name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureVertexLabel update name`(graphName: String) {
        val managementServer = createManagementServer()
        val request1 = ensureBasicVertexLabelRequest(graphName, "test1")
        val ensureVertexLabel = managementServer.ensureVertexLabel(request1)
        val request2 = ensureBasicVertexLabelRequest(graphName, "test2", ensureVertexLabel?.id?.value)

        val vertexLabel = managementServer.ensureVertexLabel(request2)

        assertEquals("test1", ensureVertexLabel?.name)
        assertEquals("test2", vertexLabel?.name)
        assertEquals(ensureVertexLabel?.id, vertexLabel?.id)
    }

    @ParameterizedTest
    @EnumSource(PropertyDataType::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `ensureVertexLabel creates property`(propertyDataType: PropertyDataType) {
        val managementServer = createManagementServer()
        val propertyName = "name"
        val property = VertexProperty.newBuilder().setName(propertyName).setDataType(propertyDataType).build()
        val request = ensureBasicVertexLabelRequest(name = "test", properties = listOf(property))

        val vertexLabel = managementServer.ensureVertexLabel(request)

        assertEquals(1, vertexLabel?.propertiesCount)
        assertEquals(propertyName, vertexLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyDataType, vertexLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureVertexLabel can run multiple times with same properties`(graphName: String) {
        val managementServer = createManagementServer()
        val propertyName = "name"
        val property = VertexProperty.newBuilder().setName(propertyName).setDataType(PropertyDataType.Boolean).build()
        val request = ensureBasicVertexLabelRequest(name = "test", properties = listOf(property))

        managementServer.ensureVertexLabel(request)

        managementServer.ensureVertexLabel(request)
        managementServer.ensureVertexLabel(request)
        val vertexLabel = managementServer.ensureVertexLabel(request)

        assertEquals(1, vertexLabel?.propertiesCount)
        assertEquals(propertyName, vertexLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(PropertyDataType.Boolean, vertexLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabelsByName no vertexLabel exists`(graphName: String) {
        val managementServer = createManagementServer()
        val request = GetVertexLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName("test").build()

        val vertexLabel = managementServer.getVertexLabelsByName(request).firstOrNull()

        assertNull(vertexLabel)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabelsByName vertexLabel exists`(graphName: String) {
        val managementServer = createManagementServer()
        val label = "test"
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest(graphName, label))
        val request = GetVertexLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName(label).build()

        val vertexLabel = managementServer.getVertexLabelsByName(request).firstOrNull()

        assertEquals(label, vertexLabel?.name)
    }

    @Test
    fun `getVertexLabelsByName vertexLabel exists not on different context`() {
        val managementServer = createManagementServer()
        val label = "test"
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest("first", label))
        val request = GetVertexLabelsByNameRequest.newBuilder()
            .setContext(createContext("second"))
            .setName(label).build()

        val vertexLabel = managementServer.getVertexLabelsByName(request).firstOrNull()

        assertNull(vertexLabel)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabelsByName update name works`(graphName: String) {
        val managementServer = createManagementServer()
        val request1 = ensureBasicVertexLabelRequest(graphName, "test1")
        val ensureEdgeLabel = managementServer.ensureVertexLabel(request1)
        val request2 = ensureBasicVertexLabelRequest(graphName, "test2", ensureEdgeLabel?.id?.value)
        managementServer.ensureVertexLabel(request2)
        val request = GetVertexLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName("test2").build()

        val vertexLabel = managementServer.getVertexLabelsByName(request).firstOrNull()

        assertEquals("test2", vertexLabel?.name)
        assertEquals(ensureEdgeLabel?.id, vertexLabel?.id)
    }

    @ParameterizedTest
    @EnumSource(PropertyDataType::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `getVertexLabelsByName returns property`(propertyDataType: PropertyDataType) {
        val managementServer = createManagementServer()
        val propertyName = "name"
        val property = VertexProperty.newBuilder().setName(propertyName).setDataType(propertyDataType).build()
        managementServer.ensureVertexLabel(
            ensureBasicVertexLabelRequest(name = "test", properties = listOf(property))
        )
        val request = GetVertexLabelsByNameRequest.newBuilder()
            .setContext(createContext("first"))
            .setName("test").build()

        val vertexLabel = managementServer.getVertexLabelsByName(request).firstOrNull()

        assertEquals(1, vertexLabel?.propertiesCount)
        assertEquals(propertyName, vertexLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyDataType, vertexLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabels return multiple vertexLabels`(graphName: String) {
        val managementServer = createManagementServer()
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest(graphName, "test1"))
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest(graphName, "test2"))
        val request = GetVertexLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val vertexLabels = managementServer.getVertexLabels(request)

        assertEquals(2, vertexLabels.size)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabels return multiple vertexLabels contains elements`(graphName: String) {
        val managementServer = createManagementServer()
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest(graphName, "test1"))
        managementServer.ensureVertexLabel(ensureBasicVertexLabelRequest(graphName, "test2"))
        val request = GetVertexLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val vertexLabels = managementServer.getVertexLabels(request)

        assertNotNull(vertexLabels.firstOrNull { it.name == "test1" })
        assertNotNull(vertexLabels.firstOrNull { it.name == "test2" })
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getVertexLabels returns multiple vertexLabels with property`(graphName: String) {
        val managementServer = createManagementServer()
        val propertyName = "name"
        val property = VertexProperty.newBuilder().setName(propertyName).setDataType(PropertyDataType.Boolean).build()
        managementServer.ensureVertexLabel(
            ensureBasicVertexLabelRequest(graphName, name = "test1", properties = listOf(property))
        )
        managementServer.ensureVertexLabel(
            ensureBasicVertexLabelRequest(graphName, name = "test2", properties = listOf(property))
        )
        val request = GetVertexLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val vertexLabels = managementServer.getVertexLabels(request)

        assertEquals(1, vertexLabels.firstOrNull { it.name == "test1" }?.propertiesCount)
        assertEquals(1, vertexLabels.firstOrNull { it.name == "test2" }?.propertiesCount)
        assertEquals(propertyName, vertexLabels.firstOrNull { it.name == "test1" }?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyName, vertexLabels.firstOrNull { it.name == "test2" }?.propertiesList?.firstOrNull()?.name)
        assertEquals(PropertyDataType.Boolean, vertexLabels.firstOrNull { it.name == "test1" }?.propertiesList?.firstOrNull()?.dataType)
        assertEquals(PropertyDataType.Boolean, vertexLabels.firstOrNull { it.name == "test2" }?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureEdgeLabel create basic edgeLabel`(graphName: String) {
        val managementServer = createManagementServer()
        val request = ensureBasicEdgeLabelRequest(graphName)

        val edgeLabel = managementServer.ensureEdgeLabel(request)

        assertEquals("test", edgeLabel?.name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureEdgeLabel can run multiple times`(graphName: String) {
        val managementServer = createManagementServer()
        val request = ensureBasicEdgeLabelRequest(graphName)

        managementServer.ensureEdgeLabel(request)
        managementServer.ensureEdgeLabel(request)
        val edgeLabel = managementServer.ensureEdgeLabel(request)

        assertEquals("test", edgeLabel?.name)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureEdgeLabel update name`(graphName: String) {
        val managementServer = createManagementServer()
        val request1 = ensureBasicEdgeLabelRequest(graphName, "test1")
        val ensureEdgeLabel = managementServer.ensureEdgeLabel(request1)
        val request2 = ensureBasicEdgeLabelRequest(graphName, "test2", ensureEdgeLabel?.id?.value)

        val edgeLabel = managementServer.ensureEdgeLabel(request2)

        assertEquals("test1", ensureEdgeLabel?.name)
        assertEquals("test2", edgeLabel?.name)
        assertEquals(ensureEdgeLabel?.id, edgeLabel?.id)
    }

    @ParameterizedTest
    @EnumSource(PropertyDataType::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `ensureEdgeLabel creates property`(propertyDataType: PropertyDataType) {
        val managementServer = createManagementServer()
        val propertyName = "propertyName"
        val property = EdgeProperty.newBuilder().setName(propertyName).setDataType(propertyDataType).build()
        val request = ensureBasicEdgeLabelRequest(name = "edgeName", properties = listOf(property))

        val edgeLabel = managementServer.ensureEdgeLabel(request)

        assertEquals(1, edgeLabel?.propertiesCount)
        assertEquals(propertyName, edgeLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyDataType, edgeLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `ensureEdgeLabel can run multiple times with same properties`(graphName: String) {
        val managementServer = createManagementServer()
        val propertyName = "propertyName"
        val property = EdgeProperty.newBuilder().setName(propertyName).setDataType(PropertyDataType.Boolean).build()
        val request = ensureBasicEdgeLabelRequest(graphName, name = "edgeName", properties = listOf(property))

        managementServer.ensureEdgeLabel(request)

        managementServer.ensureEdgeLabel(request)
        managementServer.ensureEdgeLabel(request)
        val edgeLabel = managementServer.ensureEdgeLabel(request)

        assertEquals(1, edgeLabel?.propertiesCount)
        assertEquals(propertyName, edgeLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(PropertyDataType.Boolean, edgeLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabelsByName update name works`(graphName: String) {
        val managementServer = createManagementServer()
        val request1 = ensureBasicEdgeLabelRequest(graphName, "test1")
        val ensureEdgeLabel = managementServer.ensureEdgeLabel(request1)
        val request2 = ensureBasicEdgeLabelRequest(graphName, "test2", ensureEdgeLabel?.id?.value)
        managementServer.ensureEdgeLabel(request2)
        val request = GetEdgeLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName("test2").build()

        val edgeLabel = managementServer.getEdgeLabelsByName(request).firstOrNull()

        assertEquals("test2", edgeLabel?.name)
        assertEquals(ensureEdgeLabel?.id, edgeLabel?.id)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabelsByName no edgeLabel exists`(graphName: String) {
        val managementServer = createManagementServer()
        val request = GetEdgeLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName("test").build()

        val edgeLabel = managementServer.getEdgeLabelsByName(request).firstOrNull()

        assertNull(edgeLabel)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabelsByName edgeLabel exists`(graphName: String) {
        val managementServer = createManagementServer()
        val label = "test"
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest(graphName, label))
        val request = GetEdgeLabelsByNameRequest.newBuilder()
            .setContext(createContext(graphName))
            .setName(label).build()

        val edgeLabel = managementServer.getEdgeLabelsByName(request).firstOrNull()

        assertEquals(label, edgeLabel?.name)
    }

    @Test
    fun `getEdgeLabelsByName edgeLabel exists not on different context`() {
        val managementServer = createManagementServer()
        val label = "test"
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest("first", label))
        val request = GetEdgeLabelsByNameRequest.newBuilder()
            .setContext(createContext("second"))
            .setName(label).build()

        val edgeLabel = managementServer.getEdgeLabelsByName(request).firstOrNull()

        assertNull(edgeLabel)
    }

    @ParameterizedTest
    @EnumSource(PropertyDataType::class, mode = EnumSource.Mode.EXCLUDE, names = ["UNRECOGNIZED"])
    fun `getEdgeLabelsByName returns property`(propertyDataType: PropertyDataType) {
        val managementServer = createManagementServer()
        val propertyName = "propertyName"
        val property = EdgeProperty.newBuilder().setName(propertyName).setDataType(propertyDataType).build()
        managementServer.ensureEdgeLabel(
            ensureBasicEdgeLabelRequest(name = "edgeName", properties = listOf(property))
        )
        val request = GetEdgeLabelsByNameRequest.newBuilder()
            .setContext(createContext("first"))
            .setName("edgeName").build()

        val edgeLabel = managementServer.getEdgeLabelsByName(request).firstOrNull()

        assertEquals(1, edgeLabel?.propertiesCount)
        assertEquals(propertyName, edgeLabel?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyDataType, edgeLabel?.propertiesList?.firstOrNull()?.dataType)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabels return multiple edgeLabels test size`(graphName: String) {
        val managementServer = createManagementServer()
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest(graphName, "edgeName1"))
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest(graphName, "edgeName2"))
        val request = GetEdgeLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val edgeLabels = managementServer.getEdgeLabels(request)

        assertEquals(2, edgeLabels.size)
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabels return multiple edgeLabels contains elements`(graphName: String) {
        val managementServer = createManagementServer()
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest(graphName, "edgeName1"))
        managementServer.ensureEdgeLabel(ensureBasicEdgeLabelRequest(graphName, "edgeName2"))
        val request = GetEdgeLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val edgeLabels = managementServer.getEdgeLabels(request)

        assertNotNull(edgeLabels.firstOrNull { it.name == "edgeName1" })
        assertNotNull(edgeLabels.firstOrNull { it.name == "edgeName2" })
    }

    @ParameterizedTest
    @ValueSource(strings = ["first", "second"])
    fun `getEdgeLabels returns multiple edgeLabels with property`(graphName: String) {
        val managementServer = createManagementServer()
        val propertyName = "propertyName"
        val property = EdgeProperty.newBuilder().setName(propertyName).setDataType(PropertyDataType.Boolean).build()
        managementServer.ensureEdgeLabel(
            ensureBasicEdgeLabelRequest(graphName, name = "edgeName1", properties = listOf(property))
        )
        managementServer.ensureEdgeLabel(
            ensureBasicEdgeLabelRequest(graphName, name = "edgeName2", properties = listOf(property))
        )
        val request = GetEdgeLabelsRequest.newBuilder().setContext(createContext(graphName)).build()

        val edgeLabels = managementServer.getEdgeLabels(request)

        assertEquals(1, edgeLabels.firstOrNull { it.name == "edgeName1" }?.propertiesCount)
        assertEquals(1, edgeLabels.firstOrNull { it.name == "edgeName2" }?.propertiesCount)
        assertEquals(propertyName, edgeLabels.firstOrNull { it.name == "edgeName1" }?.propertiesList?.firstOrNull()?.name)
        assertEquals(propertyName, edgeLabels.firstOrNull { it.name == "edgeName2" }?.propertiesList?.firstOrNull()?.name)
        assertEquals(PropertyDataType.Boolean, edgeLabels.firstOrNull { it.name == "edgeName1" }?.propertiesList?.firstOrNull()?.dataType)
        assertEquals(PropertyDataType.Boolean, edgeLabels.firstOrNull { it.name == "edgeName2" }?.propertiesList?.firstOrNull()?.dataType)
    }

}
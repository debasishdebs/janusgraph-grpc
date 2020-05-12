package org.janusgraph.grpc.server

import com.google.protobuf.Int64Value
import org.janusgraph.core.JanusGraph
import org.janusgraph.grpc.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path

class ManagementForPropertyKeysTests {

    private fun createDefaults(tempDir: Path? = null) =
        ManagementForPropertyKeys() to JanusGraphTestUtils.getJanusGraph(tempDir)

    private fun buildProperty(
        name: String = "propertyName",
        id: Long? = null,
        dataType: PropertyDataType = PropertyDataType.String,
        cardinality: Cardinality = Cardinality.Single
    ): PropertyKey {
        val builder = PropertyKey.newBuilder()
            .setName(name)
            .setDataType(dataType)
            .setCardinality(cardinality)
        if (id != null) {
            builder.id = Int64Value.of(id)
        }
        return builder.build()
    }

    private fun buildVertexProperty(
        name: String = "propertyName",
        id: Long? = null,
        dataType: PropertyDataType = PropertyDataType.String,
        cardinality: Cardinality = Cardinality.Single
    ): VertexProperty {
        val builder = VertexProperty.newBuilder()
            .setName(name)
            .setDataType(dataType)
            .setCardinality(cardinality)
        if (id != null) {
            builder.id = Int64Value.of(id)
        }
        return builder.build()
    }
    private fun buildEdgeProperty(
        name: String = "propertyName",
        id: Long? = null,
        dataType: PropertyDataType = PropertyDataType.String,
        cardinality: Cardinality = Cardinality.Single
    ): EdgeProperty {
        val builder = EdgeProperty.newBuilder()
            .setName(name)
            .setDataType(dataType)
        if (id != null) {
            builder.id = Int64Value.of(id)
        }
        return builder.build()
    }


    @Test
    fun `ensurePropertyKey create default property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test")

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.String, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create default list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.String, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create default set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.String, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create character datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Character)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Character, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create character datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Character)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Character, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create character datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Character)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Character, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create boolean datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Boolean)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Boolean, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create boolean datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Boolean)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Boolean, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create boolean datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Boolean)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Boolean, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int8 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Int8)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int8, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int8 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Int8)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int8, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int8 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Int8)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int8, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int16 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Int16)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int16, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int16 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Int16)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int16, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int16 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Int16)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int16, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int32 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Int32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int32, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int32 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Int32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int32, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int32 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Int32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int32, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int64 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Int64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int64, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int64 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Int64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int64, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create int64 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Int64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Int64, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float32 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Float32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float32, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float32 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Float32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float32, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float32 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Float32)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float32, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float64 datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Float64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float64, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float64 datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Float64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float64, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create float64 datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Float64)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Float64, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create date datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Date)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Date, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create date datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Date)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Date, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create date datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Date)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Date, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create javaobject datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.JavaObject)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.JavaObject, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create javaobject datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.JavaObject)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.JavaObject, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create javaobject datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.JavaObject)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.JavaObject, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create geoshape datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.GeoShape)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.GeoShape, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create geoshape datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.GeoShape)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.GeoShape, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create geoshape datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.GeoShape)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.GeoShape, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create uuid datatype property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", dataType = PropertyDataType.Uuid)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Uuid, propertyKey.dataType)
        assertEquals(Cardinality.Single, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create uuid datatype list cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.List, dataType = PropertyDataType.Uuid)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Uuid, propertyKey.dataType)
        assertEquals(Cardinality.List, propertyKey.cardinality)
    }

    @Test
    fun `ensurePropertyKey create uuid datatype set cardinality property key`() {
        val (managementServer, graph) = createDefaults()
        val property = buildProperty(name="test", cardinality = Cardinality.Set, dataType = PropertyDataType.Uuid)

        val propertyKey = managementServer.ensurePropertyKey(graph.openManagement(), property)

        assertEquals("test", propertyKey.name)
        assertNotNull(propertyKey.id)
        assertEquals(PropertyDataType.Uuid, propertyKey.dataType)
        assertEquals(Cardinality.Set, propertyKey.cardinality)
    }
}

package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaCompatibilityStrategyTest {

    @Test
    public void widensInt32ToInt64() {
        SchemaCompatibilityStrategy strategy = new SchemaCompatibilityStrategy();
        Schema resolved = strategy.compatibleSchema(
                SchemaBuilder.int32().optional().build(),
                SchemaBuilder.int64().optional().build()
        );
        assertEquals(Schema.Type.INT64, resolved.type());
    }

    @Test
    public void widensFloat32ToFloat64() {
        SchemaCompatibilityStrategy strategy = new SchemaCompatibilityStrategy();
        Schema resolved = strategy.compatibleSchema(
                SchemaBuilder.float32().optional().build(),
                SchemaBuilder.float64().optional().build()
        );
        assertEquals(Schema.Type.FLOAT64, resolved.type());
    }

    @Test
    public void upgradesDecimalScale() {
        SchemaCompatibilityStrategy strategy = new SchemaCompatibilityStrategy();
        Schema resolved = strategy.compatibleSchema(
                Decimal.builder(2).optional().build(),
                Decimal.builder(4).optional().build()
        );
        assertEquals(Decimal.LOGICAL_NAME, resolved.name());
        assertEquals("4", resolved.parameters().get(Decimal.SCALE_FIELD));
    }
}

package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class SchemaCompatibilityStrategy {

    public Schema compatibleSchema(Schema existing, Schema incoming) {
        if (existing == null) {
            return incoming;
        }
        if (existing.type() == incoming.type()) {
            if (isDecimal(existing) && isDecimal(incoming)) {
                int existingScale = Integer.parseInt(existing.parameters().get(Decimal.SCALE_FIELD));
                int incomingScale = Integer.parseInt(incoming.parameters().get(Decimal.SCALE_FIELD));
                int scale = Math.max(existingScale, incomingScale);
                return Decimal.builder(scale).optional().build();
            }
            return existing;
        }

        // Widening rules
        if (isInt32(existing) && isInt64(incoming)) {
            return SchemaBuilder.int64().optional().build();
        }
        if (isInt64(existing) && isInt32(incoming)) {
            return SchemaBuilder.int64().optional().build();
        }
        if (isFloat64(existing) && isFloat32(incoming)) {
            return SchemaBuilder.float64().optional().build();
        }
        if (isFloat32(existing) && isFloat64(incoming)) {
            return SchemaBuilder.float64().optional().build();
        }

        // Fallback: incoming wins (schema evolution)
        return incoming;
    }

    private boolean isInt32(Schema schema) {
        return schema.type() == Schema.Type.INT32;
    }

    private boolean isInt64(Schema schema) {
        return schema.type() == Schema.Type.INT64;
    }

    private boolean isFloat32(Schema schema) {
        return schema.type() == Schema.Type.FLOAT32;
    }

    private boolean isFloat64(Schema schema) {
        return schema.type() == Schema.Type.FLOAT64;
    }

    private boolean isDecimal(Schema schema) {
        return Decimal.LOGICAL_NAME.equals(schema.name());
    }
}

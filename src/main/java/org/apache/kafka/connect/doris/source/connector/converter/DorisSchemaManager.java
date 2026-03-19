package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public class DorisSchemaManager {
    private Schema cachedSchema;

    public Schema schemaFor(Map<String, Object> data) {
        if (cachedSchema == null || !isCompatible(cachedSchema, data)) {
            cachedSchema = buildSchema(data, cachedSchema);
        }
        return cachedSchema;
    }

    private boolean isCompatible(Schema schema, Map<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Field field = schema.field(entry.getKey());
            if (field == null) {
                return false;
            }
            Object value = entry.getValue();
            if (value != null) {
                Schema inferred = inferSchema(value);
                if (field.schema().type() != inferred.type()) {
                    return false;
                }
            }
        }
        return true;
    }

    private Schema buildSchema(Map<String, Object> data, Schema existingSchema) {
        Map<String, Schema> fields = new LinkedHashMap<>();
        if (existingSchema != null) {
            for (Field field : existingSchema.fields()) {
                fields.put(field.name(), field.schema());
            }
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String name = entry.getKey();
            Object value = entry.getValue();
            Schema schema = (value == null)
                    ? fields.getOrDefault(name, SchemaBuilder.string().optional().build())
                    : inferSchema(value);
            fields.put(name, schema);
        }

        SchemaBuilder builder = SchemaBuilder.struct();
        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private Schema inferSchema(Object value) {
        if (value instanceof Long) return SchemaBuilder.int64().optional().build();
        if (value instanceof Integer) return SchemaBuilder.int32().optional().build();
        if (value instanceof Boolean) return SchemaBuilder.bool().optional().build();
        if (value instanceof Double || value instanceof Float) return SchemaBuilder.float64().optional().build();
        return SchemaBuilder.string().optional().build();
    }
}

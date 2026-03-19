package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

public class DorisSchemaManager {
    private Schema cachedSchema;
    private final Map<String, Schema> schemaCache;
    private final JdbcTypeMapper jdbcTypeMapper;
    private final SchemaCompatibilityStrategy compatibilityStrategy;

    public Schema schemaFor(Map<String, Object> data) {
        String signature = signatureFor(data, null);
        Schema cached = schemaCache.get(signature);
        if (cached != null) {
            cachedSchema = cached;
            return cached;
        }
        if (cachedSchema == null || !isCompatible(cachedSchema, data)) {
            cachedSchema = buildSchema(data, cachedSchema);
        }
        schemaCache.put(signature, cachedSchema);
        return cachedSchema;
    }

    public Schema schemaFor(Map<String, Object> data, Map<String, Integer> sqlTypes) {
        String signature = signatureFor(data, sqlTypes);
        Schema cached = schemaCache.get(signature);
        if (cached != null) {
            cachedSchema = cached;
            return cached;
        }
        if (cachedSchema == null || !isCompatible(cachedSchema, data, sqlTypes)) {
            cachedSchema = buildSchema(data, existingSchemaOrNull(), sqlTypes);
        }
        schemaCache.put(signature, cachedSchema);
        return cachedSchema;
    }

    public DorisSchemaManager() {
        this(new JdbcTypeMapper(), new SchemaCompatibilityStrategy());
    }

    public DorisSchemaManager(JdbcTypeMapper jdbcTypeMapper, SchemaCompatibilityStrategy compatibilityStrategy) {
        this.jdbcTypeMapper = jdbcTypeMapper;
        this.compatibilityStrategy = compatibilityStrategy;
        this.schemaCache = new java.util.HashMap<>();
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
                if (!isCompatible(field.schema(), inferred)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isCompatible(Schema schema, Map<String, Object> data, Map<String, Integer> sqlTypes) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Field field = schema.field(entry.getKey());
            if (field == null) {
                return false;
            }
            Integer sqlType = sqlTypes.get(entry.getKey());
            if (sqlType != null) {
                Schema inferred = jdbcTypeMapper.schemaFor(sqlType, entry.getValue());
                if (!isCompatible(field.schema(), inferred)) {
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
            Schema incoming = (value == null)
                    ? fields.getOrDefault(name, SchemaBuilder.string().optional().build())
                    : inferSchema(value);
            Schema existing = fields.get(name);
            fields.put(name, compatibilityStrategy.compatibleSchema(existing, incoming));
        }

        SchemaBuilder builder = SchemaBuilder.struct();
        for (Map.Entry<String, Schema> entry : fields.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    private Schema buildSchema(Map<String, Object> data, Schema existingSchema, Map<String, Integer> sqlTypes) {
        Map<String, Schema> fields = new LinkedHashMap<>();
        if (existingSchema != null) {
            for (Field field : existingSchema.fields()) {
                fields.put(field.name(), field.schema());
            }
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String name = entry.getKey();
            Integer sqlType = sqlTypes.get(name);
            Schema incoming = (sqlType != null)
                    ? jdbcTypeMapper.schemaFor(sqlType, entry.getValue())
                    : inferSchema(entry.getValue());
            Schema existing = fields.get(name);
            fields.put(name, compatibilityStrategy.compatibleSchema(existing, incoming));
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

    private boolean isCompatible(Schema existing, Schema incoming) {
        Schema resolved = compatibilityStrategy.compatibleSchema(existing, incoming);
        return resolved != null && resolved.type() == existing.type()
                && (existing.name() == null || existing.name().equals(resolved.name()));
    }

    private Schema existingSchemaOrNull() {
        return cachedSchema;
    }

    private String signatureFor(Map<String, Object> data, Map<String, Integer> sqlTypes) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        for (String key : new java.util.TreeSet<>(data.keySet())) {
            Schema schema;
            if (sqlTypes != null && sqlTypes.containsKey(key)) {
                schema = jdbcTypeMapper.schemaFor(sqlTypes.get(key), data.get(key));
            } else {
                schema = inferSchema(data.get(key));
            }
            parts.add(key + ":" + schema.type() + ":" + (schema.name() == null ? "" : schema.name()));
        }
        return String.join("|", parts);
    }
}

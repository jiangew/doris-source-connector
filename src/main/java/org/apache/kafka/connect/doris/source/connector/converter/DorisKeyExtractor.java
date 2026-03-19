package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DorisKeyExtractor {
    private final List<String> keyColumns;
    private final DorisSchemaManager schemaManager;
    private final KeyMissingStrategy missingStrategy;

    public DorisKeyExtractor(List<String> keyColumns, KeyMissingStrategy missingStrategy) {
        this.keyColumns = keyColumns;
        this.schemaManager = new DorisSchemaManager();
        this.missingStrategy = missingStrategy;
    }

    public KeyData extract(Map<String, Object> data) {
        if (keyColumns == null || keyColumns.isEmpty()) {
            return null;
        }
        Map<String, Object> keyData = new LinkedHashMap<>();
        for (String column : keyColumns) {
            if (!data.containsKey(column)) {
                if (missingStrategy == KeyMissingStrategy.NULL) {
                    return null;
                }
                throw new IllegalArgumentException("Missing key column: " + column);
            }
            keyData.put(column, data.get(column));
        }
        Schema keySchema = schemaManager.schemaFor(keyData);
        Struct key = new Struct(keySchema);
        for (Map.Entry<String, Object> entry : keyData.entrySet()) {
            if (keySchema.field(entry.getKey()) != null) {
                key.put(entry.getKey(), entry.getValue());
            }
        }
        return new KeyData(keySchema, key);
    }

    public static class KeyData {
        private final Schema schema;
        private final Struct value;

        public KeyData(Schema schema, Struct value) {
            this.schema = schema;
            this.value = value;
        }

        public Schema getSchema() {
            return schema;
        }

        public Struct getValue() {
            return value;
        }
    }
}

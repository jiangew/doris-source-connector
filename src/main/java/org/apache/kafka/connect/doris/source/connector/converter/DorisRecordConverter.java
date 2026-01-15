package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.Map;

public class DorisRecordConverter {
    private final DorisSourceConfig config;

    public DorisRecordConverter(DorisSourceConfig config) {
        this.config = config;
    }

    public SourceRecord convert(DorisRow row, Map<String, Object> partition) {
        Schema valueSchema = createSchema(row.getData());
        Struct value = new Struct(valueSchema);
        for (Map.Entry<String, Object> entry : row.getData().entrySet()) {
            value.put(entry.getKey(), entry.getValue());
        }

        DorisSourceOffset offset = new DorisSourceOffset(row.getSeq());
        String topic = config.getDorisDatabase() + "." + config.getDorisTable();

        return new SourceRecord(
                partition,
                offset.toMap(),
                topic,
                null, // partition
                null, // key schema
                null, // key
                valueSchema,
                value
        );
    }

    private Schema createSchema(Map<String, Object> data) {
        SchemaBuilder builder = SchemaBuilder.struct();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Schema fieldSchema = inferSchema(entry.getValue());
            builder.field(entry.getKey(), fieldSchema);
        }
        return builder.build();
    }

    private Schema inferSchema(Object value) {
        if (value instanceof Long) return Schema.INT64_SCHEMA;
        if (value instanceof Integer) return Schema.INT32_SCHEMA;
        if (value instanceof Boolean) return Schema.BOOLEAN_SCHEMA;
        if (value instanceof Double || value instanceof Float) return Schema.FLOAT64_SCHEMA;
        return Schema.STRING_SCHEMA;
    }
}

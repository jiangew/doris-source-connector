package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class DorisRecordConverter {
    private final DorisSchemaManager schemaManager;
    private final DorisTopicResolver topicResolver;

    public DorisRecordConverter(DorisSourceConfig config) {
        this.schemaManager = new DorisSchemaManager();
        this.topicResolver = new DorisTopicResolver(config);
    }

    public SourceRecord convert(DorisRow row, Map<String, Object> partition) {
        Schema schema = schemaManager.schemaFor(row.getData());

        Struct value = new Struct(schema);
        for (Map.Entry<String, Object> entry : row.getData().entrySet()) {
            // Only put fields that are in the schema (to handle potential structural changes)
            if (schema.field(entry.getKey()) != null) {
                value.put(entry.getKey(), entry.getValue());
            }
        }

        DorisSourceOffset offset = new DorisSourceOffset(row.getSeq());
        String topic = topicResolver.resolve();

        return new SourceRecord(
                partition,
                offset.toMap(),
                topic,
                null, // partition
                null, // key schema
                null, // key
                schema,
                value
        );
    }
}

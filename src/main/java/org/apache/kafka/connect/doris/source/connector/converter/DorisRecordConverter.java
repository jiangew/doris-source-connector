package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public class DorisRecordConverter {
    private final DorisSchemaManager schemaManager;
    private final DorisTopicResolver topicResolver;
    private final DorisKeyExtractor keyExtractor;

    public DorisRecordConverter(DorisSourceConfig config) {
        this.schemaManager = new DorisSchemaManager();
        this.topicResolver = new DorisTopicResolver(config);
        this.keyExtractor = new DorisKeyExtractor(config.getKeyColumns());
    }

    public SourceRecord convert(DorisRow row, Map<String, Object> partition) {
        Schema schema = schemaManager.schemaFor(row.getData());
        DorisKeyExtractor.KeyData keyData = keyExtractor.extract(row.getData());

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
                keyData != null ? keyData.getSchema() : null,
                keyData != null ? keyData.getValue() : null,
                schema,
                value
        );
    }

    public SourceRecord convert(DorisRowWithTypes row, Map<String, Object> partition) {
        Schema schema = schemaManager.schemaFor(row.getData(), row.getSqlTypes());
        DorisKeyExtractor.KeyData keyData = keyExtractor.extract(row.getData());

        Struct value = new Struct(schema);
        for (Map.Entry<String, Object> entry : row.getData().entrySet()) {
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
                null,
                keyData != null ? keyData.getSchema() : null,
                keyData != null ? keyData.getValue() : null,
                schema,
                value
        );
    }
}

package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Decimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DorisRecordConverterTest {

    @Test
    public void testConvert() {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.KEY_COLUMNS, "id");
        props.put(DorisSourceConfig.TOPIC_TEMPLATE, "doris_${database}_${table}");
        DorisSourceConfig config = new DorisSourceConfig(props);

        DorisRecordConverter converter = new DorisRecordConverter(config);

        Map<String, Object> rowData = new HashMap<>();
        rowData.put("id", 1L);
        rowData.put("name", "test");
        rowData.put("seq", 100L);

        DorisRow row = new DorisRow(100L, rowData);
        Map<String, Object> partition = Collections.singletonMap("table", "tbl");

        SourceRecord record = converter.convert(row, partition);

        assertNotNull(record);
        assertEquals("doris_db_tbl", record.topic());
        assertEquals(100L, record.sourceOffset().get("last_seq"));
        assertNotNull(record.value());
        assertNotNull(record.key());
    }

    @Test
    public void evolvesSchemaWhenNewFieldAppears() {
        DorisRecordConverter converter = new DorisRecordConverter(baseConfig());

        DorisRow row1 = new DorisRow(1L, rowData(1L));
        SourceRecord record1 = converter.convert(row1, Collections.singletonMap("table", "tbl"));

        Map<String, Object> row2Data = rowData(2L);
        row2Data.put("extra", "v");
        DorisRow row2 = new DorisRow(2L, row2Data);
        SourceRecord record2 = converter.convert(row2, Collections.singletonMap("table", "tbl"));

        Schema schema1 = record1.valueSchema();
        Schema schema2 = record2.valueSchema();

        assertNotNull(schema1.field("id"));
        assertNotNull(schema2.field("extra"));
    }

    @Test
    public void usesOptionalSchemaForNullValues() {
        DorisRecordConverter converter = new DorisRecordConverter(baseConfig());

        Map<String, Object> rowData = new HashMap<>();
        rowData.put("id", 1L);
        rowData.put("name", null);
        rowData.put("seq", 10L);

        DorisRow row = new DorisRow(10L, rowData);
        SourceRecord record = converter.convert(row, Collections.singletonMap("table", "tbl"));

        Schema nameSchema = record.valueSchema().field("name").schema();
        assertTrue(nameSchema.isOptional());
    }

    @Test
    public void updatesSchemaOnTypeChange() {
        DorisRecordConverter converter = new DorisRecordConverter(baseConfig());

        Map<String, Object> row1Data = new HashMap<>();
        row1Data.put("id", 1L);
        row1Data.put("seq", 1L);
        DorisRow row1 = new DorisRow(1L, row1Data);
        SourceRecord record1 = converter.convert(row1, Collections.singletonMap("table", "tbl"));

        Map<String, Object> row2Data = new HashMap<>();
        row2Data.put("id", "1");
        row2Data.put("seq", 2L);
        DorisRow row2 = new DorisRow(2L, row2Data);
        SourceRecord record2 = converter.convert(row2, Collections.singletonMap("table", "tbl"));

        Schema idSchema1 = record1.valueSchema().field("id").schema();
        Schema idSchema2 = record2.valueSchema().field("id").schema();

        assertEquals(Schema.Type.INT64, idSchema1.type());
        assertEquals(Schema.Type.STRING, idSchema2.type());
    }

    @Test
    public void usesJdbcTypesWhenProvided() {
        DorisRecordConverter converter = new DorisRecordConverter(baseConfig());

        Map<String, Object> rowData = new HashMap<>();
        rowData.put("id", 1L);
        rowData.put("amount", new java.math.BigDecimal("12.34"));
        rowData.put("seq", 1L);

        Map<String, Integer> sqlTypes = new HashMap<>();
        sqlTypes.put("id", java.sql.Types.BIGINT);
        sqlTypes.put("amount", java.sql.Types.DECIMAL);
        sqlTypes.put("seq", java.sql.Types.BIGINT);

        org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes row =
                new org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes(1L, rowData, sqlTypes);

        SourceRecord record = converter.convert(row, Collections.singletonMap("table", "tbl"));

        Schema amountSchema = record.valueSchema().field("amount").schema();
        assertEquals(Schema.Type.BYTES, amountSchema.type());
        assertEquals(Decimal.LOGICAL_NAME, amountSchema.name());
    }

    private DorisSourceConfig baseConfig() {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        return new DorisSourceConfig(props);
    }

    private Map<String, Object> rowData(long seq) {
        Map<String, Object> rowData = new HashMap<>();
        rowData.put("id", 1L);
        rowData.put("name", "test");
        rowData.put("seq", seq);
        return rowData;
    }
}

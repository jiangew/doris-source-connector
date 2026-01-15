package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DorisRecordConverterTest {

    @Test
    public void testConvert() {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
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
        assertEquals("db.tbl", record.topic());
        assertEquals(100L, record.sourceOffset().get("last_seq"));
        assertNotNull(record.value());
    }
}

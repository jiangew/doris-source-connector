package org.apache.kafka.connect.doris.source.connector;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DorisSourceConfigTest {

    @Test
    public void testConfigParsing() {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_PORT, "9030");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");

        DorisSourceConfig config = new DorisSourceConfig(props);
        assertEquals("localhost", config.getDorisHost());
        assertEquals(9030, config.getDorisPort());
        assertEquals("root", config.getDorisUser());
        assertEquals("password", config.getDorisPassword());
        assertEquals("db", config.getDorisDatabase());
        assertEquals("tbl", config.getDorisTable());
        assertEquals("seq", config.getSeqColumn());
        assertEquals("id", config.getPartitionColumn());
        assertEquals(0, config.getKeyColumns().size());
        assertEquals("${database}.${table}", config.getTopicTemplate());
        assertEquals(1000, config.getBatchSize());
    }
}

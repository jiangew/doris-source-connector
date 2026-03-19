package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DorisTopicResolverTest {

    @Test
    public void resolvesTemplate() {
        DorisTopicResolver resolver = new DorisTopicResolver(configWithTemplate("topic_${database}_${table}"));
        assertEquals("topic_db_tbl", resolver.resolve());
    }

    @Test
    public void rejectsUnresolvedPlaceholder() {
        DorisTopicResolver resolver = new DorisTopicResolver(configWithTemplate("topic_${db}_${table}"));
        assertThrows(IllegalArgumentException.class, resolver::resolve);
    }

    @Test
    public void rejectsSpaces() {
        DorisTopicResolver resolver = new DorisTopicResolver(configWithTemplate("topic ${database}.${table}"));
        assertThrows(IllegalArgumentException.class, resolver::resolve);
    }

    private static DorisSourceConfig configWithTemplate(String template) {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.TOPIC_TEMPLATE, template);
        return new DorisSourceConfig(props);
    }
}

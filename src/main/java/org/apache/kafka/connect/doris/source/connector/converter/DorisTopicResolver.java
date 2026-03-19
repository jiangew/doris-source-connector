package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisTopicResolver {
    private final DorisSourceConfig config;

    public DorisTopicResolver(DorisSourceConfig config) {
        this.config = config;
    }

    public String resolve() {
        return config.getDorisDatabase() + "." + config.getDorisTable();
    }
}

package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisTopicResolver {
    private final DorisSourceConfig config;

    public DorisTopicResolver(DorisSourceConfig config) {
        this.config = config;
    }

    public String resolve() {
        String template = config.getTopicTemplate();
        String resolved = template
                .replace("${database}", config.getDorisDatabase())
                .replace("${table}", config.getDorisTable());
        if (resolved.trim().isEmpty()) {
            throw new IllegalArgumentException("Resolved topic must not be empty");
        }
        if (resolved.contains("${")) {
            throw new IllegalArgumentException("Unresolved placeholders in topic template: " + template);
        }
        if (resolved.contains(" ")) {
            throw new IllegalArgumentException("Resolved topic must not contain spaces: " + resolved);
        }
        return resolved;
    }
}

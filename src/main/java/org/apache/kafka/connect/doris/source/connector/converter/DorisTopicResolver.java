package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisTopicResolver {
    private final DorisSourceConfig config;
    private final TopicInvalidStrategy invalidStrategy;

    public DorisTopicResolver(DorisSourceConfig config) {
        this.config = config;
        this.invalidStrategy = TopicInvalidStrategy.fromString(config.getTopicInvalidStrategy());
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
            if (invalidStrategy == TopicInvalidStrategy.SANITIZE_SPACES) {
                resolved = resolved.replace(" ", "_");
            } else {
                throw new IllegalArgumentException("Resolved topic must not contain spaces: " + resolved);
            }
        }
        if (resolved.contains(" ")) {
            throw new IllegalArgumentException("Resolved topic must not contain spaces: " + resolved);
        }
        return resolved;
    }
}

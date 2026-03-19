package org.apache.kafka.connect.doris.source.connector.converter;

public enum TopicInvalidStrategy {
    FAIL,
    SANITIZE_SPACES;

    public static TopicInvalidStrategy fromString(String value) {
        if (value == null) {
            return FAIL;
        }
        switch (value.trim().toLowerCase()) {
            case "sanitize_spaces":
                return SANITIZE_SPACES;
            case "fail":
            default:
                return FAIL;
        }
    }
}

package org.apache.kafka.connect.doris.source.connector.converter;

public enum KeyMissingStrategy {
    FAIL,
    NULL;

    public static KeyMissingStrategy fromString(String value) {
        if (value == null) {
            return FAIL;
        }
        switch (value.trim().toLowerCase()) {
            case "null":
                return NULL;
            case "fail":
            default:
                return FAIL;
        }
    }
}

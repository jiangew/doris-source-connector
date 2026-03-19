package org.apache.kafka.connect.doris.source.connector.fetcher;

public class IdentifierEscaper {
    public String escape(String identifier) {
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier must not be empty");
        }
        String normalized = identifier.trim();
        String[] parts = normalized.split("\\.");
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                result.append('.');
            }
            result.append('`').append(parts[i].replace("`", "``")).append('`');
        }
        return result.toString();
    }
}

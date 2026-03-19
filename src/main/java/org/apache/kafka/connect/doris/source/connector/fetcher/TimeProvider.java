package org.apache.kafka.connect.doris.source.connector.fetcher;

public interface TimeProvider {
    long nowMillis();
}

package org.apache.kafka.connect.doris.source.connector.fetcher;

public class SystemTimeProvider implements TimeProvider {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}

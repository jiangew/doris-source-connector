package org.apache.kafka.connect.doris.source.connector.client;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

import java.sql.SQLException;

public class DorisClientProvider implements AutoCloseable {
    private final DorisSourceConfig config;
    private DorisClient client;

    public DorisClientProvider(DorisSourceConfig config) {
        this.config = config;
    }

    public DorisReader open() throws SQLException {
        client = new DorisClient(config);
        client.connect();
        return client;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}

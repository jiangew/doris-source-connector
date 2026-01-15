package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.client.DorisClient;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;

import java.sql.SQLException;
import java.util.List;

public class DorisIncrementalFetcher {
    private final DorisSourceConfig config;
    private final DorisClient dorisClient;

    public DorisIncrementalFetcher(DorisSourceConfig config, DorisClient dorisClient) {
        this.config = config;
        this.dorisClient = dorisClient;
    }

    public List<DorisRow> fetch(long lastSeq) throws SQLException {
        String sql = String.format(
                "SELECT * FROM %s WHERE %s > %d ORDER BY %s LIMIT %d",
                config.getDorisTable(),
                config.getSeqColumn(),
                lastSeq,
                config.getSeqColumn(),
                config.getBatchSize()
        );
        return dorisClient.fetchRecords(sql, config.getSeqColumn());
    }
}

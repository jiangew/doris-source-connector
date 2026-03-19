package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.client.DorisReader;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;

import java.sql.SQLException;
import java.util.List;

public class DorisIncrementalFetcher {
    private final DorisSourceConfig config;
    private final DorisReader dorisClient;
    private final DorisIncrementalQueryBuilder queryBuilder;

    public DorisIncrementalFetcher(DorisSourceConfig config, DorisReader dorisClient) {
        this.config = config;
        this.dorisClient = dorisClient;
        this.queryBuilder = new DorisIncrementalQueryBuilder(config);
    }

    public List<DorisRow> fetch(long lastSeq) throws SQLException {
        String sql = queryBuilder.build(lastSeq);
        return dorisClient.fetchRecords(sql, config.getSeqColumn());
    }
}

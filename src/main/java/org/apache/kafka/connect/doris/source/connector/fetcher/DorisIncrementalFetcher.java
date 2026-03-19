package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.client.DorisReader;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;

import java.sql.SQLException;
import java.util.List;

public class DorisIncrementalFetcher {
    private final DorisSourceConfig config;
    private final DorisReader dorisClient;

    public DorisIncrementalFetcher(DorisSourceConfig config, DorisReader dorisClient) {
        this.config = config;
        this.dorisClient = dorisClient;
    }

    public List<DorisRow> fetch(long lastSeq) throws SQLException {
        // Use primary key (ID) for partitioning. In a real scenario, this could be configurable.
        String partitionClause = "";
        if (config.getTaskCount() > 1) {
            partitionClause = String.format(" AND MOD(id, %d) = %d", config.getTaskCount(), config.getTaskId());
        }

        String sql = String.format(
                "SELECT * FROM %s WHERE %s > %d%s ORDER BY %s LIMIT %d",
                config.getDorisTable(),
                config.getSeqColumn(),
                lastSeq,
                partitionClause,
                config.getSeqColumn(),
                config.getBatchSize()
        );
        return dorisClient.fetchRecords(sql, config.getSeqColumn());
    }
}

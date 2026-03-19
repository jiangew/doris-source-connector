package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisIncrementalQueryBuilder {
    private final DorisSourceConfig config;

    public DorisIncrementalQueryBuilder(DorisSourceConfig config) {
        this.config = config;
    }

    public String build(long lastSeq) {
        String partitionClause = "";
        if (config.getTaskCount() > 1) {
            partitionClause = String.format(" AND MOD(%s, %d) = %d",
                    config.getPartitionColumn(),
                    config.getTaskCount(),
                    config.getTaskId());
        }

        return String.format(
                "SELECT * FROM %s WHERE %s > %d%s ORDER BY %s LIMIT %d",
                config.getDorisTable(),
                config.getSeqColumn(),
                lastSeq,
                partitionClause,
                config.getSeqColumn(),
                config.getBatchSize()
        );
    }
}

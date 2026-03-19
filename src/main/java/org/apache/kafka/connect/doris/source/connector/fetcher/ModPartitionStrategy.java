package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class ModPartitionStrategy implements PartitionStrategy {
    @Override
    public String buildClause(DorisSourceConfig config, IdentifierEscaper escaper) {
        if (config.getTaskCount() <= 1) {
            return "";
        }
        return String.format(" AND MOD(%s, %d) = %d",
                escaper.escape(config.getPartitionColumn()),
                config.getTaskCount(),
                config.getTaskId());
    }
}

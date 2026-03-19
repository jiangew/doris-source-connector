package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public interface PartitionStrategy {
    String buildClause(DorisSourceConfig config, IdentifierEscaper escaper);
}

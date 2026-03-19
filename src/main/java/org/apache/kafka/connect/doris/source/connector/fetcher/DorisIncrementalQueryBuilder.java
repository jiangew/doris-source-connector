package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisIncrementalQueryBuilder {
    private final DorisSourceConfig config;
    private final TimeProvider timeProvider;
    private final IdentifierEscaper identifierEscaper;

    public DorisIncrementalQueryBuilder(DorisSourceConfig config, TimeProvider timeProvider) {
        this.config = config;
        this.timeProvider = timeProvider;
        this.identifierEscaper = new IdentifierEscaper();
    }

    public DorisIncrementalQueryBuilder(DorisSourceConfig config) {
        this(config, new SystemTimeProvider());
    }

    public String build(long lastSeq) {
        String partitionClause = "";
        if (config.getTaskCount() > 1) {
            partitionClause = String.format(" AND MOD(%s, %d) = %d",
                    identifierEscaper.escape(config.getPartitionColumn()),
                    config.getTaskCount(),
                    config.getTaskId());
        }

        String safetyClause = buildSafetyClause();

        return String.format(
                "SELECT * FROM %s WHERE %s > %d%s%s ORDER BY %s LIMIT %d",
                identifierEscaper.escape(config.getDorisTable()),
                identifierEscaper.escape(config.getSeqColumn()),
                lastSeq,
                partitionClause,
                safetyClause,
                identifierEscaper.escape(config.getSeqColumn()),
                config.getBatchSize()
        );
    }

    private String buildSafetyClause() {
        long delayMs = config.getSafetyDelayMs();
        if (delayMs <= 0) {
            return "";
        }
        long cutoffSeconds = Math.max(0L, (timeProvider.nowMillis() - delayMs) / 1000L);
        return String.format(" AND %s < FROM_UNIXTIME(%d)", identifierEscaper.escape(config.getUpdateTimeColumn()), cutoffSeconds);
    }
}

package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class DorisIncrementalQueryBuilder {
    private final DorisSourceConfig config;
    private final TimeProvider timeProvider;
    private final IdentifierEscaper identifierEscaper;
    private final SafetyWindowPredicate safetyPredicate;
    private final PartitionStrategy partitionStrategy;

    public DorisIncrementalQueryBuilder(DorisSourceConfig config, TimeProvider timeProvider) {
        this.config = config;
        this.timeProvider = timeProvider;
        this.identifierEscaper = new IdentifierEscaper();
        this.safetyPredicate = new SafetyDelayPredicate(config, timeProvider);
        this.partitionStrategy = selectPartitionStrategy(config);
    }

    public DorisIncrementalQueryBuilder(DorisSourceConfig config) {
        this(config, new SystemTimeProvider());
    }

    public String build(long lastSeq) {
        String partitionClause = partitionStrategy.buildClause(config, identifierEscaper);

        String safetyClause = safetyPredicate.build();

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

    private PartitionStrategy selectPartitionStrategy(DorisSourceConfig config) {
        String strategy = config.getPartitionStrategy();
        if ("mod".equalsIgnoreCase(strategy)) {
            return new ModPartitionStrategy();
        }
        return new ModPartitionStrategy();
    }

}

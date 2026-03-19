package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;

public class SafetyDelayPredicate implements SafetyWindowPredicate {
    private final DorisSourceConfig config;
    private final TimeProvider timeProvider;

    public SafetyDelayPredicate(DorisSourceConfig config, TimeProvider timeProvider) {
        this.config = config;
        this.timeProvider = timeProvider;
    }

    @Override
    public String build() {
        long delayMs = config.getSafetyDelayMs();
        if (delayMs <= 0) {
            return "";
        }
        long cutoffSeconds = Math.max(0L, (timeProvider.nowMillis() - delayMs) / 1000L);
        return String.format(" AND %s < FROM_UNIXTIME(%d)", new IdentifierEscaper().escape(config.getUpdateTimeColumn()), cutoffSeconds);
    }
}

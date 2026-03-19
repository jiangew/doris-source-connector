package org.apache.kafka.connect.doris.source.connector.sync;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.doris.source.connector.converter.DorisRecordConverter;
import org.apache.kafka.connect.doris.source.connector.fetcher.DorisIncrementalFetcher;
import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultSyncEngine implements SyncEngine {
    private static final Logger log = LoggerFactory.getLogger(DefaultSyncEngine.class);

    private final DorisSourceConfig config;
    private final DorisIncrementalFetcher fetcher;
    private final DorisRecordConverter converter;
    private final Map<String, Object> partition;
    private final Runnable onClose;
    private DorisSourceOffset currentOffset;

    public DefaultSyncEngine(
            DorisSourceConfig config,
            DorisIncrementalFetcher fetcher,
            DorisRecordConverter converter,
            DorisSourceOffset initialOffset,
            Map<String, Object> partition,
            Runnable onClose
    ) {
        this.config = config;
        this.fetcher = fetcher;
        this.converter = converter;
        this.currentOffset = initialOffset;
        this.partition = partition;
        this.onClose = onClose;
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            List<DorisRowWithTypes> rows = fetcher.fetchWithTypes(currentOffset.getLastSeq());
            if (rows.isEmpty()) {
                log.debug("No new records for task {}, sleeping for {}ms", config.getTaskId(), config.getPollIntervalMs());
                Thread.sleep(config.getPollIntervalMs());
                return null;
            }

            log.info("Task {} fetched {} records", config.getTaskId(), rows.size());
            List<SourceRecord> records = new ArrayList<>(rows.size());
            for (DorisRowWithTypes row : rows) {
                records.add(converter.convert(row, partition));
                currentOffset = new DorisSourceOffset(row.getSeq());
            }
            return records;
        } catch (SQLException e) {
            log.error("Failed to fetch records from Doris in task " + config.getTaskId(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public DorisSourceOffset currentOffset() {
        return currentOffset;
    }

    @Override
    public void close() {
        if (onClose != null) {
            onClose.run();
        }
    }
}

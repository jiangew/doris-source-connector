package org.apache.kafka.connect.doris.source.connector;

import org.apache.kafka.connect.doris.source.connector.client.DorisClient;
import org.apache.kafka.connect.doris.source.connector.converter.DorisRecordConverter;
import org.apache.kafka.connect.doris.source.connector.fetcher.DorisIncrementalFetcher;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(DorisSourceTask.class);

    private DorisSourceConfig config;
    private DorisClient dorisClient;
    private DorisIncrementalFetcher fetcher;
    private DorisRecordConverter converter;
    private DorisSourceOffset currentOffset;
    private Map<String, Object> partition;

    @Override
    public String version() {
        return "0.1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new DorisSourceConfig(props);
        this.dorisClient = new DorisClient(config);
        log.info("Starting task {}/{} for table {}", config.getTaskId(), config.getTaskCount(), config.getDorisTable());
        try {
            this.dorisClient.connect();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to Doris", e);
        }
        this.fetcher = new DorisIncrementalFetcher(config, dorisClient);
        this.converter = new DorisRecordConverter(config);

        // Include task info in partition to ensure each task has its own offset stream
        this.partition = new HashMap<>();
        partition.put("table", config.getDorisTable());
        partition.put("task_id", String.valueOf(config.getTaskId()));
        partition.put("task_count", String.valueOf(config.getTaskCount()));

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(partition);
        this.currentOffset = DorisSourceOffset.fromMap(lastOffset);
        log.info("Task {} initialized with offset {}", config.getTaskId(), currentOffset.getLastSeq());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            List<DorisRow> rows = fetcher.fetch(currentOffset.getLastSeq());
            if (rows.isEmpty()) {
                log.debug("No new records for task {}, sleeping for {}ms", config.getTaskId(), config.getPollIntervalMs());
                Thread.sleep(config.getPollIntervalMs());
                return null;
            }

            log.info("Task {} fetched {} records", config.getTaskId(), rows.size());
            List<SourceRecord> records = new ArrayList<>(rows.size());
            for (DorisRow row : rows) {
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
    public void stop() {
        if (dorisClient != null) {
            dorisClient.close();
        }
    }
}

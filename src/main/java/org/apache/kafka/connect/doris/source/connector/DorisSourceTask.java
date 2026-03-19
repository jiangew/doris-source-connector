package org.apache.kafka.connect.doris.source.connector;

import org.apache.kafka.connect.doris.source.connector.client.DorisClientProvider;
import org.apache.kafka.connect.doris.source.connector.client.DorisReader;
import org.apache.kafka.connect.doris.source.connector.converter.DorisRecordConverter;
import org.apache.kafka.connect.doris.source.connector.fetcher.DorisIncrementalFetcher;
import org.apache.kafka.connect.doris.source.connector.sync.DefaultSyncEngine;
import org.apache.kafka.connect.doris.source.connector.sync.SyncEngine;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(DorisSourceTask.class);

    private DorisSourceConfig config;
    private DorisClientProvider dorisClientProvider;
    private Map<String, Object> partition;
    private SyncEngine syncEngine;

    @Override
    public String version() {
        return "0.1.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new DorisSourceConfig(props);
        log.info("Starting task {}/{} for table {}", config.getTaskId(), config.getTaskCount(), config.getDorisTable());
        DorisReader dorisReader;
        try {
            this.dorisClientProvider = new DorisClientProvider(config);
            dorisReader = this.dorisClientProvider.open();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to Doris", e);
        }
        DorisIncrementalFetcher fetcher = new DorisIncrementalFetcher(config, dorisReader);
        DorisRecordConverter converter = new DorisRecordConverter(config);

        // Include task info in partition to ensure each task has its own offset stream
        this.partition = new HashMap<>();
        partition.put("table", config.getDorisTable());
        partition.put("task_id", String.valueOf(config.getTaskId()));
        partition.put("task_count", String.valueOf(config.getTaskCount()));

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(partition);
        DorisSourceOffset initialOffset = DorisSourceOffset.fromMap(lastOffset);
        log.info("Task {} initialized with offset {}", config.getTaskId(), initialOffset.getLastSeq());
        this.syncEngine = new DefaultSyncEngine(
                config,
                fetcher,
                converter,
                initialOffset,
                partition,
                dorisClientProvider::close
        );
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return syncEngine.poll();
    }

    @Override
    public void stop() {
        if (syncEngine != null) {
            syncEngine.close();
            return;
        }
        if (dorisClientProvider != null) {
            dorisClientProvider.close();
        }
    }
}

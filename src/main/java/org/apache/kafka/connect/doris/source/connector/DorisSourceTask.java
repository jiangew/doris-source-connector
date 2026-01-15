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
        try {
            this.dorisClient.connect();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to Doris", e);
        }
        this.fetcher = new DorisIncrementalFetcher(config, dorisClient);
        this.converter = new DorisRecordConverter(config);

        this.partition = Collections.singletonMap("table", config.getDorisTable());

        Map<String, Object> lastOffset = context.offsetStorageReader().offset(partition);
        this.currentOffset = DorisSourceOffset.fromMap(lastOffset);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            List<DorisRow> rows = fetcher.fetch(currentOffset.getLastSeq());
            if (rows.isEmpty()) {
                Thread.sleep(5000); // Wait for new data
                return null;
            }

            List<SourceRecord> records = new ArrayList<>(rows.size());
            for (DorisRow row : rows) {
                records.add(converter.convert(row, partition));
                currentOffset = new DorisSourceOffset(row.getSeq());
            }
            return records;
        } catch (SQLException e) {
            log.error("Failed to fetch records from Doris", e);
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

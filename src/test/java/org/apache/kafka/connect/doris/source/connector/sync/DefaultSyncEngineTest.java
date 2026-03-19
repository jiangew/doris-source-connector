package org.apache.kafka.connect.doris.source.connector.sync;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.doris.source.connector.client.DorisReader;
import org.apache.kafka.connect.doris.source.connector.converter.DorisRecordConverter;
import org.apache.kafka.connect.doris.source.connector.fetcher.DorisIncrementalFetcher;
import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultSyncEngineTest {

    @Test
    public void returnsNullAndPreservesOffsetWhenNoRows() throws InterruptedException {
        DorisSourceConfig config = configWithPollInterval(100);
        DorisReader reader = new DorisReader() {
            @Override
            public List<org.apache.kafka.connect.doris.source.connector.model.DorisRow> fetchRecords(String sql, String seqColumn) {
                return Collections.emptyList();
            }

            @Override
            public List<DorisRowWithTypes> fetchRecordsWithTypes(String sql) {
                return Collections.emptyList();
            }
        };
        DorisIncrementalFetcher fetcher = new DorisIncrementalFetcher(config, reader);
        DorisRecordConverter converter = new DorisRecordConverter(config);
        DorisSourceOffset initialOffset = new DorisSourceOffset(-1L);
        Map<String, Object> partition = Collections.singletonMap("table", "tbl");
        AtomicBoolean closed = new AtomicBoolean(false);

        DefaultSyncEngine engine = new DefaultSyncEngine(
                config,
                fetcher,
                converter,
                initialOffset,
                partition,
                () -> closed.set(true)
        );

        List<SourceRecord> records = engine.poll();

        assertNull(records);
        assertEquals(-1L, engine.currentOffset().getLastSeq());

        engine.close();
        assertTrue(closed.get());
    }

    @Test
    public void updatesOffsetAndReturnsRecords() throws InterruptedException {
        DorisSourceConfig config = configWithPollInterval(100);
        DorisReader reader = new DorisReader() {
            @Override
            public List<org.apache.kafka.connect.doris.source.connector.model.DorisRow> fetchRecords(String sql, String seqColumn) {
                return Collections.emptyList();
            }

            @Override
            public List<DorisRowWithTypes> fetchRecordsWithTypes(String sql) {
                return Arrays.asList(
                        new DorisRowWithTypes(100L, rowData(100L), sqlTypes()),
                        new DorisRowWithTypes(200L, rowData(200L), sqlTypes())
                );
            }
        };
        DorisIncrementalFetcher fetcher = new DorisIncrementalFetcher(config, reader);
        DorisRecordConverter converter = new DorisRecordConverter(config);
        DorisSourceOffset initialOffset = new DorisSourceOffset(50L);
        Map<String, Object> partition = Collections.singletonMap("table", "tbl");

        DefaultSyncEngine engine = new DefaultSyncEngine(
                config,
                fetcher,
                converter,
                initialOffset,
                partition,
                null
        );

        List<SourceRecord> records = engine.poll();

        assertEquals(2, records.size());
        assertEquals(200L, engine.currentOffset().getLastSeq());
    }

    @Test
    public void wrapsSQLExceptionFromReader() {
        DorisSourceConfig config = configWithPollInterval(100);
        DorisReader reader = new DorisReader() {
            @Override
            public List<org.apache.kafka.connect.doris.source.connector.model.DorisRow> fetchRecords(String sql, String seqColumn) throws SQLException {
                throw new SQLException("boom");
            }

            @Override
            public List<DorisRowWithTypes> fetchRecordsWithTypes(String sql) throws SQLException {
                throw new SQLException("boom");
            }
        };
        DorisIncrementalFetcher fetcher = new DorisIncrementalFetcher(config, reader);
        DorisRecordConverter converter = new DorisRecordConverter(config);
        DorisSourceOffset initialOffset = new DorisSourceOffset(0L);
        Map<String, Object> partition = Collections.singletonMap("table", "tbl");

        DefaultSyncEngine engine = new DefaultSyncEngine(
                config,
                fetcher,
                converter,
                initialOffset,
                partition,
                null
        );

        assertThrows(RuntimeException.class, engine::poll);
    }

    private static DorisSourceConfig configWithPollInterval(long pollIntervalMs) {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.POLL_INTERVAL_MS, String.valueOf(pollIntervalMs));
        return new DorisSourceConfig(props);
    }

    private static Map<String, Object> rowData(long seq) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L);
        row.put("name", "test");
        row.put("seq", seq);
        return row;
    }

    private static Map<String, Integer> sqlTypes() {
        Map<String, Integer> types = new HashMap<>();
        types.put("id", java.sql.Types.BIGINT);
        types.put("name", java.sql.Types.VARCHAR);
        types.put("seq", java.sql.Types.BIGINT);
        return types;
    }
}

package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.client.DorisReader;
import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DorisIncrementalFetcherTest {

    @Test
    public void passesConfiguredSeqColumnToReader() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.SEQ_COLUMN, "sequence");
        DorisSourceConfig config = new DorisSourceConfig(props);

        DorisReader reader = new DorisReader() {
            @Override
            public List<org.apache.kafka.connect.doris.source.connector.model.DorisRow> fetchRecords(String sql, String seqColumn) {
                return Collections.emptyList();
            }

            @Override
            public List<DorisRowWithTypes> fetchRecordsWithTypes(String sql, String seqColumn) {
                assertEquals("sequence", seqColumn);
                return Collections.emptyList();
            }
        };

        DorisIncrementalFetcher fetcher = new DorisIncrementalFetcher(config, reader);
        fetcher.fetchWithTypes(0L);
    }
}

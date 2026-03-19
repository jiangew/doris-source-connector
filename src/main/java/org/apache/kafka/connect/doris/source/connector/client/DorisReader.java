package org.apache.kafka.connect.doris.source.connector.client;

import org.apache.kafka.connect.doris.source.connector.model.DorisRow;

import java.sql.SQLException;
import java.util.List;

public interface DorisReader {
    List<DorisRow> fetchRecords(String sql, String seqColumn) throws SQLException;

    default List<org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes> fetchRecordsWithTypes(String sql) throws SQLException {
        throw new UnsupportedOperationException("fetchRecordsWithTypes not supported");
    }
}

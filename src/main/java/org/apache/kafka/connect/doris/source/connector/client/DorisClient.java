package org.apache.kafka.connect.doris.source.connector.client;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.apache.kafka.connect.doris.source.connector.model.DorisRow;
import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisClient implements DorisReader {
    private static final Logger log = LoggerFactory.getLogger(DorisClient.class);

    private final DorisSourceConfig config;
    private Connection connection;
    private final DorisRowMapper rowMapper;

    public DorisClient(DorisSourceConfig config) {
        this.config = config;
        this.rowMapper = new DorisRowMapper();
    }

    public void connect() throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/%s",
                config.getDorisHost(), config.getDorisPort(), config.getDorisDatabase());
        log.info("Connecting to Doris at {}", url);
        connection = DriverManager.getConnection(url, config.getDorisUser(), config.getDorisPassword());
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("Failed to close Doris connection", e);
            }
        }
    }

    @Override
    public List<DorisRow> fetchRecords(String sql, String seqColumn) throws SQLException {
        List<DorisRow> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> data = new HashMap<>();
                long seq = -1;
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    data.put(columnName, value);
                    if (columnName.equalsIgnoreCase(seqColumn)) {
                        seq = rs.getLong(i);
                    }
                }
                rows.add(new DorisRow(seq, data));
            }
        }
        return rows;
    }

    @Override
    public List<DorisRowWithTypes> fetchRecordsWithTypes(String sql, String seqColumn) throws SQLException {
        List<DorisRowWithTypes> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(rowMapper.map(rs, seqColumn));
            }
        }
        return rows;
    }
}

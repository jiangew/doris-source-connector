package org.apache.kafka.connect.doris.source.connector.client;

import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DorisRowMapper {
    public DorisRowWithTypes map(ResultSet rs, String seqColumn) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        Map<String, Object> data = new HashMap<>();
        Map<String, Integer> sqlTypes = new HashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            data.put(columnName, value);
            sqlTypes.put(columnName, metaData.getColumnType(i));
        }

        long seq = -1;
        Object seqValue = data.get(seqColumn);
        if (seqValue instanceof Number) {
            seq = ((Number) seqValue).longValue();
        }
        return new DorisRowWithTypes(seq, data, sqlTypes);
    }
}

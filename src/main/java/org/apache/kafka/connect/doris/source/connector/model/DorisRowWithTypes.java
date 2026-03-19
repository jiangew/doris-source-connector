package org.apache.kafka.connect.doris.source.connector.model;

import java.sql.Types;
import java.util.Collections;
import java.util.Map;

public class DorisRowWithTypes {
    private final long seq;
    private final Map<String, Object> data;
    private final Map<String, Integer> sqlTypes;

    public DorisRowWithTypes(long seq, Map<String, Object> data, Map<String, Integer> sqlTypes) {
        this.seq = seq;
        this.data = data;
        this.sqlTypes = sqlTypes;
    }

    public long getSeq() {
        return seq;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Map<String, Integer> getSqlTypes() {
        return Collections.unmodifiableMap(sqlTypes);
    }
}

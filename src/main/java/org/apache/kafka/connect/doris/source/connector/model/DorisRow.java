package org.apache.kafka.connect.doris.source.connector.model;

import java.util.Map;

public class DorisRow {
    private final long seq;
    private final Map<String, Object> data;

    public DorisRow(long seq, Map<String, Object> data) {
        this.seq = seq;
        this.data = data;
    }

    public long getSeq() {
        return seq;
    }

    public Map<String, Object> getData() {
        return data;
    }
}

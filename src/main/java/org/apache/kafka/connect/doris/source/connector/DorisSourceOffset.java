package org.apache.kafka.connect.doris.source.connector;

import java.util.Collections;
import java.util.Map;

public class DorisSourceOffset {
    private final long lastSeq;

    public DorisSourceOffset(long lastSeq) {
        this.lastSeq = lastSeq;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public Map<String, Object> toMap() {
        return Collections.singletonMap("last_seq", lastSeq);
    }

    public static DorisSourceOffset fromMap(Map<String, Object> offsetMap) {
        if (offsetMap == null || !offsetMap.containsKey("last_seq")) {
            return new DorisSourceOffset(-1L);
        }
        return new DorisSourceOffset(((Number) offsetMap.get("last_seq")).longValue());
    }
}

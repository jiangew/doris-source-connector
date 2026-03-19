package org.apache.kafka.connect.doris.source.connector.sync;

import org.apache.kafka.connect.doris.source.connector.DorisSourceOffset;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface SyncEngine {
    List<SourceRecord> poll() throws InterruptedException;
    DorisSourceOffset currentOffset();
    void close();
}

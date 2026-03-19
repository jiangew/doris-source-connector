package org.apache.kafka.connect.doris.source.connector.sync;

import java.util.concurrent.Callable;

public interface RetryPolicy {
    <T> T execute(Callable<T> action);
}

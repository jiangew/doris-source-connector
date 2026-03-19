package org.apache.kafka.connect.doris.source.connector.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class FixedBackoffRetryPolicy implements RetryPolicy {
    private static final Logger log = LoggerFactory.getLogger(FixedBackoffRetryPolicy.class);

    private final int maxAttempts;
    private final long backoffMs;

    public FixedBackoffRetryPolicy(int maxAttempts, long backoffMs) {
        this.maxAttempts = Math.max(0, maxAttempts);
        this.backoffMs = Math.max(0L, backoffMs);
    }

    @Override
    public <T> T execute(Callable<T> action) {
        int attempts = 0;
        while (true) {
            try {
                return action.call();
            } catch (Exception e) {
                attempts++;
                if (attempts > maxAttempts) {
                    throw new RuntimeException(e);
                }
                log.warn("Retry attempt {}/{} after error: {}", attempts, maxAttempts, e.toString());
                sleepQuietly(backoffMs);
            }
        }
    }

    private void sleepQuietly(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }
}

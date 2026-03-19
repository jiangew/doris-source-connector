package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DorisKeyExtractorTest {

    @Test
    public void extractsKeyWhenColumnsPresent() {
        DorisKeyExtractor extractor = new DorisKeyExtractor(Arrays.asList("id", "tenant"), KeyMissingStrategy.FAIL);
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);
        data.put("tenant", "t1");

        DorisKeyExtractor.KeyData keyData = extractor.extract(data);

        assertNotNull(keyData);
        Struct key = keyData.getValue();
        assertEquals(1L, key.get("id"));
        assertEquals("t1", key.get("tenant"));
    }

    @Test
    public void throwsWhenKeyColumnMissing() {
        DorisKeyExtractor extractor = new DorisKeyExtractor(Arrays.asList("id", "tenant"), KeyMissingStrategy.FAIL);
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);

        assertThrows(IllegalArgumentException.class, () -> extractor.extract(data));
    }

    @Test
    public void returnsNullWhenStrategyAllowsMissingKeys() {
        DorisKeyExtractor extractor = new DorisKeyExtractor(Arrays.asList("id", "tenant"), KeyMissingStrategy.NULL);
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1L);

        DorisKeyExtractor.KeyData keyData = extractor.extract(data);
        assertEquals(null, keyData);
    }
}

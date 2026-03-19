package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.apache.kafka.connect.doris.source.connector.DorisSourceConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DorisIncrementalQueryBuilderTest {

    @Test
    public void buildsQueryWithoutPartitionWhenSingleTask() {
        DorisIncrementalQueryBuilder builder = new DorisIncrementalQueryBuilder(config(0, 1, "id"), () -> 0L);
        String sql = builder.build(100L);
        assertEquals(
                "SELECT * FROM `tbl` WHERE `seq` > 100 ORDER BY `seq` LIMIT 1000",
                sql
        );
    }

    @Test
    public void buildsQueryWithPartitionWhenMultipleTasks() {
        DorisIncrementalQueryBuilder builder = new DorisIncrementalQueryBuilder(config(2, 4, "user_id"), () -> 0L);
        String sql = builder.build(5L);
        assertEquals(
                "SELECT * FROM `tbl` WHERE `seq` > 5 AND MOD(`user_id`, 4) = 2 ORDER BY `seq` LIMIT 1000",
                sql
        );
    }

    @Test
    public void buildsQueryWithSafetyDelay() {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.SEQ_COLUMN, "seq");
        props.put(DorisSourceConfig.PARTITION_COLUMN, "id");
        props.put(DorisSourceConfig.UPDATE_TIME_COLUMN, "update_time");
        props.put(DorisSourceConfig.SAFETY_DELAY_MS, "5000");
        props.put(DorisSourceConfig.TASK_ID, "0");
        props.put(DorisSourceConfig.TASK_COUNT, "1");
        DorisSourceConfig config = new DorisSourceConfig(props);

        DorisIncrementalQueryBuilder builder = new DorisIncrementalQueryBuilder(config, () -> 1_700_000_000_000L);
        String sql = builder.build(1L);

        assertEquals(
                "SELECT * FROM `tbl` WHERE `seq` > 1 AND `update_time` < FROM_UNIXTIME(1699999995) ORDER BY `seq` LIMIT 1000",
                sql
        );
    }

    private static DorisSourceConfig config(int taskId, int taskCount, String partitionColumn) {
        Map<String, String> props = new HashMap<>();
        props.put(DorisSourceConfig.DORIS_HOST, "localhost");
        props.put(DorisSourceConfig.DORIS_USER, "root");
        props.put(DorisSourceConfig.DORIS_PASSWORD, "password");
        props.put(DorisSourceConfig.DORIS_DATABASE, "db");
        props.put(DorisSourceConfig.DORIS_TABLE, "tbl");
        props.put(DorisSourceConfig.SEQ_COLUMN, "seq");
        props.put(DorisSourceConfig.PARTITION_COLUMN, partitionColumn);
        props.put(DorisSourceConfig.BATCH_SIZE, "1000");
        props.put(DorisSourceConfig.UPDATE_TIME_COLUMN, "update_time");
        props.put(DorisSourceConfig.TASK_ID, String.valueOf(taskId));
        props.put(DorisSourceConfig.TASK_COUNT, String.valueOf(taskCount));
        return new DorisSourceConfig(props);
    }
}

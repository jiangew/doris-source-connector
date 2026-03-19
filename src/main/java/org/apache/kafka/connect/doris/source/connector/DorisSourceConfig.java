package org.apache.kafka.connect.doris.source.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;

import java.util.Map;

public class DorisSourceConfig extends AbstractConfig {

    public static final String DORIS_HOST = "doris.host";
    public static final String DORIS_PORT = "doris.port";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    public static final String DORIS_DATABASE = "doris.database";
    public static final String DORIS_TABLE = "doris.table";
    public static final String SEQ_COLUMN = "seq.column";
    public static final String PARTITION_COLUMN = "partition.column";
    public static final String KEY_COLUMNS = "key.columns";
    public static final String TOPIC_TEMPLATE = "topic.template";
    public static final String BATCH_SIZE = "batch.size";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";

    public static final String TASK_ID = "task.id";
    public static final String TASK_COUNT = "task.count";

    public DorisSourceConfig(ConfigDef config, Map<String, ?> parsedConfig) {
        super(config, parsedConfig);
    }

    public DorisSourceConfig(Map<String, ?> parsedConfig) {
        this(CONFIG_DEF, parsedConfig);
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connection
            .define(DORIS_HOST, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), Importance.HIGH, "Doris host", "Connection", 1, ConfigDef.Width.MEDIUM, "Doris Host")
            .define(DORIS_PORT, Type.INT, 9030, Range.atLeast(1), Importance.HIGH, "Doris query port", "Connection", 2, ConfigDef.Width.SHORT, "Doris Port")
            .define(DORIS_USER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), Importance.HIGH, "Doris user", "Connection", 3, ConfigDef.Width.MEDIUM, "Doris User")
            .define(DORIS_PASSWORD, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Doris password", "Connection", 4, ConfigDef.Width.MEDIUM, "Doris Password")
            .define(DORIS_DATABASE, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), Importance.HIGH, "Doris database", "Connection", 5, ConfigDef.Width.MEDIUM, "Doris Database")
            .define(DORIS_TABLE, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyString(), Importance.HIGH, "Doris table", "Connection", 6, ConfigDef.Width.MEDIUM, "Doris Table")
            // Synchronization
            .define(SEQ_COLUMN, Type.STRING, "seq", new NonEmptyString(), Importance.MEDIUM, "Sequence column for incremental fetch", "Synchronization", 1, ConfigDef.Width.MEDIUM, "Sequence Column")
            .define(PARTITION_COLUMN, Type.STRING, "id", new NonEmptyString(), Importance.MEDIUM, "Partition column for task sharding", "Synchronization", 2, ConfigDef.Width.MEDIUM, "Partition Column")
            .define(KEY_COLUMNS, Type.LIST, "", Importance.MEDIUM, "Key columns (comma-separated) for SourceRecord key", "Synchronization", 3, ConfigDef.Width.MEDIUM, "Key Columns")
            .define(TOPIC_TEMPLATE, Type.STRING, "${database}.${table}", Importance.MEDIUM, "Topic template using ${database} and ${table}", "Synchronization", 4, ConfigDef.Width.MEDIUM, "Topic Template")
            .define(BATCH_SIZE, Type.INT, 1000, Range.between(1, 100000), Importance.MEDIUM, "Fetch batch size", "Synchronization", 5, ConfigDef.Width.SHORT, "Batch Size")
            .define(POLL_INTERVAL_MS, Type.LONG, 5000L, Range.atLeast(100), Importance.LOW, "Poll interval in milliseconds", "Synchronization", 6, ConfigDef.Width.SHORT, "Poll Interval")
            // Internal
            .define(TASK_ID, Type.INT, 0, Importance.LOW, "Internal Task ID")
            .define(TASK_COUNT, Type.INT, 1, Importance.LOW, "Internal Task Count");

    public String getDorisHost() { return getString(DORIS_HOST); }
    public int getDorisPort() { return getInt(DORIS_PORT); }
    public String getDorisUser() { return getString(DORIS_USER); }
    public String getDorisPassword() { return getPassword(DORIS_PASSWORD).value(); }
    public String getDorisDatabase() { return getString(DORIS_DATABASE); }
    public String getDorisTable() { return getString(DORIS_TABLE); }
    public String getSeqColumn() { return getString(SEQ_COLUMN); }
    public String getPartitionColumn() { return getString(PARTITION_COLUMN); }
    public java.util.List<String> getKeyColumns() { return getList(KEY_COLUMNS); }
    public String getTopicTemplate() { return getString(TOPIC_TEMPLATE); }
    public int getBatchSize() { return getInt(BATCH_SIZE); }
    public long getPollIntervalMs() { return getLong(POLL_INTERVAL_MS); }
    public int getTaskId() { return getInt(TASK_ID); }
    public int getTaskCount() { return getInt(TASK_COUNT); }
}

package org.apache.kafka.connect.doris.source.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisSourceConnector extends SourceConnector {

    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DorisSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProperties);
            taskConfig.put(DorisSourceConfig.TASK_ID, String.valueOf(i));
            taskConfig.put(DorisSourceConfig.TASK_COUNT, String.valueOf(maxTasks));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return DorisSourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "0.1.0-SNAPSHOT";
    }
}

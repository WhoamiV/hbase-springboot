package com.example.test.hbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.context.annotation.Configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import java.util.Map;
import java.util.Set;

@Configuration
@EnableConfigurationProperties(HBaseProperties.class)
public class HBaseConfig {
    private final HBaseProperties properties;
    public HBaseConfig(HBaseProperties properties) {
        this.properties = properties;
    }
    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }
        return configuration;
    }
}

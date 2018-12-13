package com.example.test.hbase.config;

/**
 * Created by fantastyJ on 2018/12/13 3:59 PM
 */
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Map;

@ConfigurationProperties(prefix = "hbase")
public class HBaseProperties {
    private Map<String, String> config;
    public Map<String, String> getConfig() {
        return config;
    }
    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}


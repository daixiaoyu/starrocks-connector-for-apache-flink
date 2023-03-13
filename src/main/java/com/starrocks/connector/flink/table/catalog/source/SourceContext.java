package com.starrocks.connector.flink.table.catalog.source;

import java.util.HashMap;
import java.util.Map;

public class SourceContext {
    private String scanUrl;
    private Map<String,String> config = new HashMap<>();

    public void addConfig(String key,String value){
        this.config.put(key,value);
    }
    public void setScanUrl(String scanUrl) {
        this.scanUrl = scanUrl;
    }

    public String getScanUrl() {
        return scanUrl;
    }

    public Map<String, String> getConfig() {
        return config;
    }
}

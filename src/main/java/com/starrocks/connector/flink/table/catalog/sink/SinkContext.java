package com.starrocks.connector.flink.table.catalog.sink;

import java.util.HashMap;
import java.util.Map;

public class SinkContext {
    private String loadUrl;

    private Map<String,String> config = new HashMap<>();

    public void addConfig(String key,String value){
        this.config.put(key,value);
    }
    public void setLoadUrl(String loadUrl) {
        this.loadUrl = loadUrl;
    }

    public String getLoadUrl() {
        return loadUrl;
    }

    public Map<String, String> getConfig() {
        return config;
    }
}

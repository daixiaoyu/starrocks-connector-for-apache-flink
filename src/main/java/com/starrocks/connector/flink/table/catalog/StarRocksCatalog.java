package com.starrocks.connector.flink.table.catalog;

import com.starrocks.connector.flink.table.catalog.sink.SinkContext;
import com.starrocks.connector.flink.table.catalog.source.SourceContext;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicTableSinkFactory;
import com.starrocks.connector.flink.table.source.StarRocksDynamicTableSourceFactory;
import org.apache.flink.connector.jdbc.catalog.MySqlCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.LOAD_URL;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.*;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.PASSWORD;
import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.*;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import org.apache.flink.table.factories.CatalogFactory.Context;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

public class StarRocksCatalog extends MySqlCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    private CatalogType catalogType;

    private SourceContext sourceContext;
    private SinkContext sinkContext;

    private Context context;

    public StarRocksCatalog(ClassLoader userClassLoader,
                            String catalogName,
                            String defaultDatabase,
                            String username,
                            String pwd,
                            String baseUrl,
                            SourceContext sourceContext,
                            SinkContext sinkContext,
                            CatalogType catalogType,
                            Context context) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
        this.catalogType = catalogType;
        this.sourceContext = sourceContext;
        this.sinkContext = sinkContext;
        this.context = context;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        CatalogBaseTable table = super.getTable(tablePath);

        Map<String, String> props = table.getOptions();
        props.remove(URL.key());
        props.put(CONNECTOR.key(), "starrocks");
        props.put(JDBC_URL.key(), baseUrl);
        props.put(USERNAME.key(), username);
        props.put(PASSWORD.key(), pwd);
        props.put(DATABASE_NAME.key(), getSchemaName(tablePath));
        props.put(TABLE_NAME.key(), getSchemaTableName(tablePath));
        if (catalogType.equals(CatalogType.SOURCE)) {
            props.put(SCAN_URL.key(), sourceContext.getScanUrl());
            props.putAll(sourceContext.getConfig());
        } else {
            props.put(LOAD_URL.key(), sinkContext.getLoadUrl());
            props.putAll(sinkContext.getConfig());
        }

        return table;
    }

    @Override
    public Optional<Factory> getFactory() {
        if (catalogType.equals(CatalogType.SOURCE)) {
            return Optional.of(new StarRocksDynamicTableSourceFactory());
        } else {
            return Optional.of(new StarRocksDynamicTableSinkFactory());
        }
    }
}

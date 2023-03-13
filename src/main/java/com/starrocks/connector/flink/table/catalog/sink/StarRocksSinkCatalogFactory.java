package com.starrocks.connector.flink.table.catalog.sink;


import com.starrocks.connector.flink.table.catalog.CatalogType;
import com.starrocks.connector.flink.table.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.table.catalog.StarRocksCatalogFactoryOptions;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.*;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;


public class StarRocksSinkCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return StarRocksCatalogFactoryOptions.SINK_IDENTIFIER;
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(LOAD_URL_STRING);
        options.add(JDBC_URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(SINK_BATCH_MAX_ROWS);
        options.add(SINK_BATCH_MAX_SIZE);
        options.add(SINK_BATCH_FLUSH_INTERVAL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        SinkContext sinkContext = new SinkContext();
        sinkContext.setLoadUrl(helper.getOptions().get(LOAD_URL_STRING));
        sinkContext.addConfig(SINK_BATCH_MAX_ROWS.key(),helper.getOptions().get(SINK_BATCH_MAX_ROWS).toString());
        sinkContext.addConfig(SINK_BATCH_MAX_SIZE.key(),helper.getOptions().get(SINK_BATCH_MAX_SIZE).toString());
        sinkContext.addConfig(SINK_BATCH_FLUSH_INTERVAL.key(),helper.getOptions().get(SINK_BATCH_FLUSH_INTERVAL).toString());

        sinkContext.addConfig("sink.properties.ignore_json_size","true");
        sinkContext.addConfig("sink.properties.format","json");
        sinkContext.addConfig("sink.properties.strip_outer_array","true");
        sinkContext.addConfig("sink.properties.timeout","1800");
        sinkContext.addConfig("sink.version","V1");

        return new StarRocksCatalog(
                context.getClassLoader(),
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(JDBC_URL),
                null,
                sinkContext,
                CatalogType.SINK,
                context);
    }
}

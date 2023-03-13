package com.starrocks.connector.flink.table.catalog.source;


import com.starrocks.connector.flink.table.catalog.CatalogType;
import com.starrocks.connector.flink.table.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.table.catalog.StarRocksCatalogFactoryOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static com.starrocks.connector.flink.table.source.StarRocksSourceOptions.*;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;


public class StarRocksSourceCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return StarRocksCatalogFactoryOptions.SOURCE_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(SCAN_URL);
        options.add(JDBC_URL);
        options.add(USERNAME);
        options.add(PASSWORD);
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
        SourceContext sourceContext = new SourceContext();
        sourceContext.setScanUrl(helper.getOptions().get(SCAN_URL));

        return new StarRocksCatalog(
                context.getClassLoader(),
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(JDBC_URL),
                sourceContext,
                null,
                CatalogType.SOURCE,
                context);
    }
}

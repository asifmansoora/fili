// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.wiki.webservice.data.config.table;

import com.yahoo.bard.webservice.data.config.ResourceDictionaries;
import com.yahoo.bard.webservice.data.config.dimension.DimensionConfig;
import com.yahoo.bard.webservice.data.config.names.ApiMetricName;
import com.yahoo.bard.webservice.data.config.names.FieldName;
import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.data.config.table.BaseTableLoader;
import com.yahoo.bard.webservice.data.config.table.ConcretePhysicalTableDefinition;
import com.yahoo.bard.webservice.data.config.table.PhysicalTableDefinition;
import com.yahoo.bard.webservice.data.time.TimeGrain;
import com.yahoo.bard.webservice.data.time.ZonedTimeGrain;
import com.yahoo.bard.webservice.data.time.ZonelessTimeGrain;
import com.yahoo.bard.webservice.druid.model.query.AllGranularity;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.TableGroup;
import com.yahoo.wiki.webservice.data.config.auto.DataSourceConfiguration;
import com.yahoo.wiki.webservice.data.config.dimension.GenericDimensionConfigs;
import com.yahoo.wiki.webservice.data.config.metric.DruidMetricName;
import com.yahoo.wiki.webservice.data.config.metric.FiliApiMetricName;

import org.joda.time.DateTimeZone;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

/**
 * Load the table configuration for any druid setup.
 */
public class GenericTableLoader extends BaseTableLoader {
    private Set<Granularity> validGrains = new HashSet<>();
    // Set up the metrics
    private Set<FieldName> druidMetricNames = new HashSet<>();
    private Set<ApiMetricName> apiMetricNames = new HashSet<>();
    // Set up the table definitions
    private Set<PhysicalTableDefinition> tableDefinitions = new HashSet<>();
    private final Supplier<List<? extends DataSourceConfiguration>> configLoader;

    /**
     * Constructor.
     *
     * @param configLoader  Gives a list of {@link DataSourceConfiguration} to build tables from.
     * @param genericDimensionConfigs  Reference to the already constructed dimensions.
     * @param metadataService  Service containing the segment data for constructing tables.
     */
    public GenericTableLoader(
            @NotNull Supplier<List<? extends DataSourceConfiguration>> configLoader,
            @NotNull GenericDimensionConfigs genericDimensionConfigs,
            DataSourceMetadataService metadataService
    ) {
        super(metadataService);
        this.configLoader = configLoader;
        configureTables(genericDimensionConfigs);
    }

    /**
     * Set up the tables for this table loader.
     *
     * @param genericDimensionConfigs  The dimensions to load into test tables.
     */
    private void configureTables(GenericDimensionConfigs genericDimensionConfigs) {
        configLoader.get().forEach(dataSourceConfiguration -> {

            druidMetricNames = dataSourceConfiguration.getMetrics()
                    .stream()
                    .map(DruidMetricName::new)
                    .collect(Collectors.toSet());

            tableDefinitions = getPhysicalTableDefinitions(
                    dataSourceConfiguration,
                    dataSourceConfiguration.getValidTimeGrain(),
                    genericDimensionConfigs.getAllDimensionConfigurations()
            );

            apiMetricNames = dataSourceConfiguration.getMetrics()
                    .stream()
                    .map(metricName -> new FiliApiMetricName(metricName, dataSourceConfiguration.getValidTimeGrain()))
                    .collect(Collectors.toSet());

            validGrains = getGranularities(dataSourceConfiguration);
        });

    }

    /**
     * Creates a set of valid granularities from valid timegrains.
     *
     * @param dataSourceConfiguration  Reference to datasource configuration.
     *
     * @return set of valid granularities.
     */
    private Set<Granularity> getGranularities(DataSourceConfiguration dataSourceConfiguration) {
        Set<Granularity> granularities = new LinkedHashSet<>();
        granularities.add(AllGranularity.INSTANCE);
        granularities.add(dataSourceConfiguration.getValidTimeGrain());
        return granularities;
    }

    /**
     * Creates a {@link PhysicalTableDefinition} definitions.
     *
     * @param dataSourceConfiguration  DataSourceConfiguration to build physical table definition from.
     * @param timeGrain  Valid timegrain for table to be created.
     * @param dimsBasefactDruidTable  Base dimensions to be built into PhysicalTableDefinition.
     *
     * @return set of PhysicalTableDefinition for the datasource.
     */
    private Set<PhysicalTableDefinition> getPhysicalTableDefinitions(
            DataSourceConfiguration dataSourceConfiguration,
            TimeGrain timeGrain,
            Set<DimensionConfig> dimsBasefactDruidTable
    ) {
        ZonedTimeGrain zonedTimeGrain = new ZonedTimeGrain(
                (ZonelessTimeGrain) timeGrain,
                DateTimeZone.UTC
        );
        return new LinkedHashSet<>(
                Collections.singletonList(
                        new ConcretePhysicalTableDefinition(
                                dataSourceConfiguration.getTableName(),
                                zonedTimeGrain,
                                druidMetricNames,
                                dimsBasefactDruidTable
                        )
                )
        );
    }

    /**
     * Load each logical table from the datasources given.
     *
     * @param dictionaries  ResourceDictionaries to load with each table.
     */
    @Override
    public void loadTableDictionary(ResourceDictionaries dictionaries) {
        configLoader.get()
                .forEach(table -> {
                    Set<TableName> currentTableGroupTableNames = tableDefinitions.stream()
                            .map(PhysicalTableDefinition::getName)
                            .collect(Collectors.toSet());
                    TableGroup tableGroup = buildDimensionSpanningTableGroup(
                            currentTableGroupTableNames,
                            tableDefinitions,
                            dictionaries,
                            apiMetricNames
                    );
                    loadLogicalTableWithGranularities(
                            table.getTableName().asName(),
                            tableGroup,
                            validGrains,
                            dictionaries
                    );
                });
    }
}

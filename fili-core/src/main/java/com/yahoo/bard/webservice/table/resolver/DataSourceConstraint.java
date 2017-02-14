// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.metric.LogicalMetric;
import com.yahoo.bard.webservice.data.metric.TemplateDruidQuery;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.druid.model.query.Granularity;
import com.yahoo.bard.webservice.table.LogicalTable;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.Interval;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Constraints used to filter and resolve tables being selected.
 */
public class DataSourceConstraint {

    LogicalTable logicalTable;
    Granularity requestGranularity;
    Granularity metricGranularity;
    Granularity minumumTimeGran;
    Set<Dimension> requestDimensions;
    Set<Dimension> filterDimensions;
    Set<Dimension> metricDimensions;
    Set<Interval> intervals;
    Set<LogicalMetric> logicalMetrics;
    Set<String> metricNames;

    /**
     * Constructor.
     *
     * @param dataApiRequest Api request containing the constraints information.
     * @param templateDruidQuery Query containing metric constraint information.
     */
    public DataSourceConstraint(DataApiRequest dataApiRequest, DruidAggregationQuery<?> templateDruidQuery) {
        if (templateDruidQuery instanceof TemplateDruidQuery) {
            this.minumumTimeGran = new RequestQueryGranularityResolver().apply(
                    dataApiRequest,
                    (TemplateDruidQuery) templateDruidQuery
            );
        }
        this.logicalTable = dataApiRequest.getTable();
        this.requestGranularity = dataApiRequest.getGranularity();
        this.metricGranularity = templateDruidQuery.getGranularity();
        this.requestDimensions = dataApiRequest.getDimensions();
        this.intervals = dataApiRequest.getIntervals();
        this.filterDimensions = dataApiRequest.getFilterDimensions();
        this.metricDimensions = templateDruidQuery.getInnermostQuery().getMetricDimensions();
        this.logicalMetrics = dataApiRequest.getLogicalMetrics();
        this.metricNames = templateDruidQuery.getInnermostQuery().getDependentFieldNames();
    }

    public Set<String> getRequestDimensionNames() {
        return getRequestDimensions().stream().map(Dimension::getApiName).collect(Collectors.toSet());
    }

    public Set<Dimension> getFilterDimensions() {
        return filterDimensions;
    }

    public Set<Dimension> getMetricDimensions() {
        return metricDimensions;
    }

    public LogicalTable getLogicalTable() {
        return logicalTable;
    }

    public Granularity getMinimumTimeGran() {
        return minumumTimeGran;
    }

    public Granularity getRequestGranularity() {
        return requestGranularity;
    }

    public Granularity getMetricGranularity() {
        return metricGranularity;
    }

    public Set<Dimension> getRequestDimensions() {
        return requestDimensions;
    }

    public Set<Dimension> getAllDimensions() {
        return Stream.of(
                getRequestDimensions().stream(),
                getFilterDimensions().stream(),
                getMetricDimensions().stream()
        ).flatMap(Function.identity()).collect(Collectors.toSet());
    }

    public Set<String> getAllDimensionNames() {
        return getAllDimensions().stream().map(Dimension::getApiName).collect(Collectors.toSet());
    }

    public Set<String> getAllColumnNames() {
        return Stream.of(
                getAllDimensionNames().stream(),
                getMetricNames().stream()
        ).flatMap(Function.identity()).collect(Collectors.toSet());
    }

    public Set<String> getMetricNames() {
        return metricNames;
    }

    public Set<Interval> getIntervals() {
        return intervals;
    }

    public Set<LogicalMetric> getLogicalMetrics() {
        return logicalMetrics;
    }

    public Set<String> getLogicalMetricNames() {
        return getLogicalMetrics().stream().map(LogicalMetric::getName).collect(Collectors.toSet());
    }
}

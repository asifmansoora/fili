// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.resolver;

import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;
import com.yahoo.bard.webservice.web.DataApiRequest;

import org.joda.time.Interval;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Constraints used to filter and resolve tables being selected.
 */
public class QueryPlanningConstraint extends DataSourceConstraint {

    Set<Interval> intervals;

    /**
     * Constructor.
     *
     * @param dataApiRequest Api request containing the constraints information.
     * @param templateDruidQuery Query containing metric constraint information.
     */
    public QueryPlanningConstraint(DataApiRequest dataApiRequest, DruidAggregationQuery<?> templateDruidQuery) {
        super(dataApiRequest, templateDruidQuery);
        this.intervals = dataApiRequest.getIntervals();
    }

    public Set<String> getRequestDimensionNames() {
        return getRequestDimensions().stream().map(Dimension::getApiName).collect(Collectors.toSet());
    }

    public Set<Interval> getIntervals() {
        return intervals;
    }
}

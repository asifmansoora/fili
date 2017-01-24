// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.table.Column;

import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Availability describes the intervals available by column for a table.
 */
public interface Availability {

    List<Interval> get(Column column);

    default Supplier<List<Interval>> getAvailabilitySupplier(Column column) {
        return () -> get(column);
    }

    Collection<List<Interval>> values();

    Set<Column> keySet();

    default Map<Column, List<Interval>> getIdealAvailability() {
        return keySet().stream()
                .collect(Collectors.toMap(Function.identity(), this::get));
    }

    default Map<Column, Supplier<List<Interval>>> getFilteredAvailabilitySupplier(Class<? extends Column> columnClass) {
        return keySet().stream()
                .filter(it-> columnClass.isAssignableFrom(it.getClass()))
                .collect(Collectors.toMap(Function.identity(), this::getAvailabilitySupplier));
    }

    /**
     * Fetch a set of intervals given a column name.
     *
     * @param columnName  Name of the column
     *
     * @return Set of intervals associated with a column, empty if column is missing
     */
    default List<Interval> getIntervalsByColumnName(String columnName) {
        List<Interval> result = get(new Column(columnName));
        if (result != null) {
            return result;
        }
        return Collections.emptyList();
    }
}

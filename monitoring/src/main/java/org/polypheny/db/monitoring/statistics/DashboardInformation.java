/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.statistics;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.util.Pair;


public class DashboardInformation {

    @Setter
    @Getter
    private int numberOfCommits;

    @Setter
    @Getter
    private int numberOfRollbacks;

    @Setter
    @Getter
    private int numberOfQueries;

    @Setter
    @Getter
    private int numberOfWorkloads;

    @Setter
    @Getter
    private long numberOfPendingEvents;

    @Getter
    private final Map<String, Pair<String, AdapterType>> availableAdapter = new HashMap<>();

    @Getter
    private final Map<Long, Pair<String, NamespaceType>> availableSchemas = new HashMap<>();

    @Getter
    private boolean catalogPersistent;


    public DashboardInformation() {
        this.numberOfCommits = 0;
        this.numberOfRollbacks = 0;
        this.numberOfQueries = 0;
        this.numberOfWorkloads = 0;
        this.numberOfPendingEvents = 0;

        updatePolyphenyStatistic();
    }


    public void updatePolyphenyStatistic() {
        Catalog catalog = Catalog.getInstance();
        this.catalogPersistent = catalog.isPersistent;

        this.numberOfQueries = MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPointImpl.class ).size();
        this.numberOfWorkloads = MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class ).size();
        this.numberOfPendingEvents = MonitoringServiceProvider.getInstance().getNumberOfElementsInQueue();

        catalog.getAdapters().forEach( v -> {
            this.availableAdapter.put( v.uniqueName, Pair.of( v.getAdapterTypeName(), v.type ) );
        } );
        catalog.getSchemas( null, null ).forEach( v -> {
            availableSchemas.put( v.id, Pair.of( v.name, v.namespaceType ) );
        } );
    }

}

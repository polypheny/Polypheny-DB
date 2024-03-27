/*
 * Copyright 2019-2024 The Polypheny Project
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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.util.Pair;


@Getter
public class DashboardInformation {

    @JsonProperty
    @Setter
    private int numberOfCommits = 0;

    @JsonProperty
    @Setter
    private int numberOfRollbacks = 0;

    @JsonProperty
    @Setter
    private int numberOfQueries = 0;

    @JsonProperty
    @Setter
    private int numberOfWorkloads = 0;

    @JsonProperty
    @Setter
    private long numberOfPendingEvents = 0;

    @JsonProperty
    private Map<String, Pair<String, AdapterType>> availableAdapter = new HashMap<>();

    @JsonProperty
    private Map<String, DataModel> availableNamespaces = new HashMap<>();

    @JsonProperty
    private boolean catalogPersistent;


    public DashboardInformation() {
        updateStatistic();
    }


    public void updateStatistic() {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        this.catalogPersistent = Catalog.getInstance().isPersistent;

        this.numberOfQueries = MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPointImpl.class ).size();
        this.numberOfWorkloads = MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class ).size();
        this.numberOfPendingEvents = MonitoringServiceProvider.getInstance().getNumberOfElementsInQueue();

        this.availableAdapter = snapshot.getAdapters().stream().collect( Collectors.toMap( v -> v.uniqueName, v -> Pair.of( v.adapterTypeName, v.type ) ) );
        this.availableNamespaces = snapshot.getNamespaces( null ).stream().collect( Collectors.toMap( v -> v.name, v -> v.dataModel ) );
    }

}

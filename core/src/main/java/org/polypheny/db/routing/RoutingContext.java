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

package org.polypheny.db.routing;

import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.Statement;

@Value
public class RoutingContext {

    AlgCluster cluster;
    Statement statement;
    LogicalQueryInformation queryInformation;

    @Nullable
    @NonFinal
    public FieldDistribution fieldDistribution; // PartitionId -> List<AllocationColumn>

    @Nullable
    @NonFinal
    public RoutedDistribution routedDistribution; // PartitionId -> List<AdapterId, CatalogColumnPlacementId>


    public RoutingContext( AlgCluster cluster, Statement statement, LogicalQueryInformation queryInformation ) {
        this.cluster = cluster;
        this.statement = statement;
        this.queryInformation = queryInformation;
    }


    public RoutedAlgBuilder getRoutedAlgBuilder() {
        return RoutedAlgBuilder.create( statement, cluster );
    }


    public Snapshot getSnapshot() {
        return cluster.getSnapshot();
    }

}

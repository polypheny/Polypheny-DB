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

package org.polypheny.db.replication.freshness;


import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.replication.freshness.properties.FreshnessSpecification;


/**
 * This class is used to transform and extract relevant information out of a query which specified the optional acceptance to retrieve outdated data.
 */
public abstract class FreshnessManager {


    public abstract double transformToFreshnessIndex( CatalogTable table, String s, EvaluationType evaluationType );


    public abstract boolean checkFreshnessConstraints( FreshnessSpecification requiredFreshnessSpecification, List<CatalogPartitionPlacement> orderedPartitionPlacements );

    /**
     * Gets a list of ALL CatalogPartitionPlacements based on their freshness for a given table.
     * Returns multiple/redundant partitions and only filters if it matches the tolerated freshness
     *
     * @param table Table to query
     * @param specs FreshnessMetrics to consider
     * @return A Map of partition id to List of usable partitionPlacements that all tolerate the refresh operations
     */
    public abstract Map<Long, List<CatalogPartitionPlacement>> getRelevantPartitionPlacements( CatalogTable table, List<Long> partitionIds, FreshnessSpecification specs );


    public enum EvaluationType {
        TIMESTAMP,
        DELAY,
        PERCENTAGE,
        INDEX,
        TIME_DEVIATION
    }

}

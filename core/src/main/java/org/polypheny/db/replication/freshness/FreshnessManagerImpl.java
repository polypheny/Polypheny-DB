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


import com.google.common.collect.ImmutableList;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.DataPlacementRole;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeRuntimeException;
import org.polypheny.db.replication.freshness.properties.FreshnessSpecification;


public class FreshnessManagerImpl extends FreshnessManager {

    Catalog catalog = Catalog.getInstance();


    @Override
    public double transformToFreshnessIndex( CatalogTable table, String s, EvaluationType evaluationType ) {
        return 0;
    }


    @Override
    public Map<Long, List<CatalogPartitionPlacement>> getRelevantPartitionPlacements( CatalogTable table, List<Long> partitionIds, FreshnessSpecification specs ) {
        return filterOnFreshness( getAllRefreshablePlacements( table, partitionIds ), specs );
    }


    private Map<Long, List<CatalogPartitionPlacement>> getAllRefreshablePlacements( CatalogTable table, List<Long> partitionIds ) {
        return catalog.getPartitionPlacementsByIdAndRole( table.id, partitionIds, DataPlacementRole.REFRESHABLE );
    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnFreshness(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            FreshnessSpecification specs ) {

        switch ( specs.getEvaluationType() ) {

            case TIMESTAMP:
            case DELAY:
                return filterOnTimestampFreshness( unfilteredPlacements, specs.getToleratedTimestamp() );

            case PERCENTAGE:
            case INDEX:
                return filterOnFreshnessIndex( unfilteredPlacements, specs.getFreshnessIndex() );

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( specs.getEvaluationType().toString() );
        }

    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnTimestampFreshness(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            Timestamp toleratedTimestampFilter ) {

        Map<Long, List<CatalogPartitionPlacement>> filteredPlacements = new HashMap<>();

        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {

            long partitionId = entry.getKey();
            List<CatalogPartitionPlacement> partitionPlacements = entry.getValue();

            // Only keep placements that are newer than the specified threshold
            partitionPlacements.stream().filter(
                    partitionPlacement -> toleratedTimestampFilter.before( partitionPlacement.updateTimestamp )
            );

            filteredPlacements.put( partitionId, ImmutableList.copyOf( partitionPlacements ) );
        }

        return filteredPlacements;

    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnFreshnessIndex(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            double freshnessIndexFilter ) {

        Map<Long, List<CatalogPartitionPlacement>> filteredPlacements = new HashMap<>();

        return filteredPlacements;
    }

}

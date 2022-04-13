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
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
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


    /**
     * @return true if plan should be removed
     */
    @Override
    public boolean checkFreshnessConstraints( FreshnessSpecification requiredFreshnessSpecification, List<CatalogPartitionPlacement> orderedPartitionPlacements ) {

        // Cannot compare freshness originating from different evaluation types.
        if ( orderedPartitionPlacements.isEmpty() ) {
            return true;
        }

        switch ( requiredFreshnessSpecification.getEvaluationType() ) {

            // TODO @HENNLO Differentiate between DELAY from the Current TimeStamp = Timestamp parsing
            //  or between delay from the commitTimestamp of the eager placement vs.the candidate Lazy placement

            // Remove if given is older(occured before) than required
            case TIMESTAMP:
            case DELAY:
                return verifyTimestampFreshnessConstraint( requiredFreshnessSpecification, orderedPartitionPlacements );

            case TIME_DEVIATION:
                return false;

            case PERCENTAGE:
            case INDEX:
                return false;

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( requiredFreshnessSpecification.getEvaluationType().toString() );
        }

    }


    public boolean verifyTimestampFreshnessConstraint( FreshnessSpecification requiredFreshnessSpecification, List<CatalogPartitionPlacement> orderedPartitionPlacements ) {

        // The list or placements was originally ordered by the oldest placement at the top
        // Therefore, it is sufficient to check whether this oldest placement currently supports the requirement or not ( with the state that it has when it was cached
        // Even if it was updated in teh meantime
        if ( requiredFreshnessSpecification.getToleratedTimestamp().before( new Timestamp( orderedPartitionPlacements.get( 0 ).getCommitTimestamp() ) ) ) {
            return false;

            // If this is not the case we need to check every utilized placement if it currently  is able to fulfil the requirement
        } else {
            for ( CatalogPartitionPlacement partitionPlacement : orderedPartitionPlacements ) {
                // How
                if ( requiredFreshnessSpecification.getToleratedTimestamp().after( new Timestamp( partitionPlacement.getCommitTimestamp() ) ) ) {
                    return true;
                }
            }
            // If all placements have been successfully checked we can use this plan.
            return false;
        }
    }


    @Override
    public Map<Long, List<CatalogPartitionPlacement>> getRelevantPartitionPlacements( CatalogTable table, List<Long> partitionIds, FreshnessSpecification specs ) {
        return filterOnFreshness( table, getAllRefreshablePlacements( table, partitionIds ), specs );
    }


    private Map<Long, List<CatalogPartitionPlacement>> getAllRefreshablePlacements( CatalogTable table, List<Long> partitionIds ) {

        Map<Long, List<CatalogPartitionPlacement>> refreshablePlacementsByPartitionId = new HashMap<>();
        for ( long partitionId : partitionIds ) {
            refreshablePlacementsByPartitionId.put( partitionId, catalog.getPartitionPlacementsByIdAndReplicationStrategy( table.id, partitionId, ReplicationStrategy.LAZY ) );
        }

        return refreshablePlacementsByPartitionId;
    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnFreshness(
            CatalogTable table,
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            FreshnessSpecification specs ) {

        switch ( specs.getEvaluationType() ) {

            // TODO @HENNLO Differentiate between DELAY from the Current TimeStamp = Timestamp parsing
            //  or between delay from the commitTimestamp of the eager placement vs.the candidate Lazy placement

            case TIMESTAMP:
            case DELAY:
                return filterOnTimestampFreshness( unfilteredPlacements, specs.getToleratedTimestamp() );

            case TIME_DEVIATION:
                return filterOnTimeDeviationFreshness( table, unfilteredPlacements );

            case PERCENTAGE:
            case INDEX:
                return filterOnFreshnessIndex( unfilteredPlacements, specs.getFreshnessIndex() );

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( specs.getEvaluationType().toString() );
        }

    }


    /**
     * Filters placements based on their deviation from the master
     * Acts as a wrapper/ pre-filter before calling {{@link #filterOnTimestampFreshness(Map, Timestamp)}}
     */
    private Map<Long, List<CatalogPartitionPlacement>> filterOnTimeDeviationFreshness( CatalogTable table, Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements ) {

        Map<Long, List<CatalogPartitionPlacement>> filteredPlacementsPerPartition = new HashMap<>();

        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {

            long partitionId = entry.getKey();
            List<CatalogPartitionPlacement> placements = entry.getValue();

            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacementsPerPartition = new HashMap<>();
            unfilteredPlacementsPerPartition.put( partitionId, placements );

            // Retrieves one of the EAGERly updated placements to get the corresponding update Timestamp
            Timestamp commitTimestampOfEagerPartition = new Timestamp(
                    catalog.getPartitionPlacementsByIdAndReplicationStrategy( table.id, partitionId, ReplicationStrategy.EAGER ).get( 0 ).updateInformation.commitTimestamp
            );

            // Filters only fraction of entire unfiltered map and then filters this sub list based on their identifiers
            // Than adds this to global map.
            filteredPlacementsPerPartition.putAll( filterOnTimestampFreshness( unfilteredPlacementsPerPartition, commitTimestampOfEagerPartition ) );
        }

        return filteredPlacementsPerPartition;
    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnTimestampFreshness(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            Timestamp toleratedTimestampFilter ) {

        Map<Long, List<CatalogPartitionPlacement>> filteredPlacements = new HashMap<>();

        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {

            long partitionId = entry.getKey();
            List<CatalogPartitionPlacement> partitionPlacements = entry.getValue();

            // TODO @HENNLO CHeck if this is indeed BEFORE
            // Only keep placements that are newer than the specified threshold
           /*partitionPlacements.stream().filter(
                    partitionPlacement -> toleratedTimestampFilter.before( new Timestamp( partitionPlacement.updateInformation.commitTimestamp ) )
            );*/
            // Removes all Data Placements that are older than the toleratedTimestamp Value
            partitionPlacements.removeIf( partitionPlacement -> toleratedTimestampFilter.after( new Timestamp( partitionPlacement.updateInformation.commitTimestamp ) ) );
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

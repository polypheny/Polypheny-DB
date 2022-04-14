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


import java.sql.Timestamp;
import java.util.Arrays;
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
            case ABSOLUTE_DELAY:
                return verifyTimestampFreshnessConstraint( requiredFreshnessSpecification, orderedPartitionPlacements );

            case RELATIVE_DELAY:
                return verifyTimeDeviationFreshnessConstraint( requiredFreshnessSpecification, orderedPartitionPlacements );

            case PERCENTAGE:
            case INDEX:
                return verifyModificationDeviationFreshnessConstraint( requiredFreshnessSpecification, orderedPartitionPlacements );

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


    private boolean verifyTimeDeviationFreshnessConstraint( FreshnessSpecification requiredFreshnessSpecification, List<CatalogPartitionPlacement> orderedPartitionPlacements ) {

        for ( CatalogPartitionPlacement partitionPlacement : orderedPartitionPlacements ) {

            long partitionId = partitionPlacement.partitionId;
            // Retrieves one of the EAGERly updated placements to get the corresponding update Timestamp
            long commitTimestampOfEagerPartition =
                    catalog.getPartitionPlacementsByIdAndReplicationStrategy( partitionPlacement.tableId, partitionId, ReplicationStrategy.EAGER ).get( 0 ).updateInformation.commitTimestamp;

            Timestamp commitTimestampOfEagerPartition2 = new Timestamp( commitTimestampOfEagerPartition - requiredFreshnessSpecification.getTimeDelay() );

            // If the EAGERly replicated node is newer than the current Partitions CommitTimestamp even WITH the freshness tolerance time delay
            if ( commitTimestampOfEagerPartition2.after( new Timestamp( partitionPlacement.getCommitTimestamp() ) ) ) {
                return true;
            }
        }

        return false;
    }


    /**
     * Calculates a freshnessIndexFilter based on the number of modifications
     */
    private boolean verifyModificationDeviationFreshnessConstraint( FreshnessSpecification requiredFreshnessSpecification, List<CatalogPartitionPlacement> orderedPartitionPlacements ) {

        long jointEagerPartitionModifications = 0;
        long jointLazyPartitionModifications = 0;
        boolean compareLocally = false;

        for ( CatalogPartitionPlacement partitionPlacement : orderedPartitionPlacements ) {

            long partitionId = partitionPlacement.partitionId;
            // Retrieves one of the EAGERly updated placements to get the corresponding update Timestamp
            jointEagerPartitionModifications +=
                    catalog.getPartitionPlacementsByIdAndReplicationStrategy( partitionPlacement.tableId, partitionId, ReplicationStrategy.EAGER ).get( 0 ).getModifications();

            jointLazyPartitionModifications += partitionPlacement.getModifications();

            // If enabled checks if each participating Placement matched the freshnessConstraints
            if ( compareLocally ) {
                // Build local Index
                double actualLocalFreshnessIndex = jointLazyPartitionModifications / jointEagerPartitionModifications;
                // If it is not accepted
                if ( actualLocalFreshnessIndex < requiredFreshnessSpecification.getFreshnessIndex() ) {
                    return true;
                }
            }
        }

        // Build global Index
        double actualFreshnessIndex = (double) jointLazyPartitionModifications / (double) jointEagerPartitionModifications;

        // If it is not accepted
        if ( actualFreshnessIndex < requiredFreshnessSpecification.getFreshnessIndex() ) {
            return true;
        }

        return false;
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
            case ABSOLUTE_DELAY:
                return filterOnTimestampFreshness( unfilteredPlacements, specs.getToleratedTimestamp() );

            case RELATIVE_DELAY:
                return filterOnTimeDeviationFreshness( table, unfilteredPlacements, specs );

            case PERCENTAGE:
            case INDEX:
                return filterOnFreshnessIndex( unfilteredPlacements, specs );

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( specs.getEvaluationType().toString() );
        }

    }


    /**
     * Filters placements based on their deviation from the master
     * Acts as a wrapper/ pre-filter before calling {{@link #filterOnTimestampFreshness(Map, Timestamp)}}
     */
    private Map<Long, List<CatalogPartitionPlacement>> filterOnTimeDeviationFreshness( CatalogTable table, Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements, FreshnessSpecification specs ) {

        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {

            List<CatalogPartitionPlacement> placements = entry.getValue();
            placements.removeIf( partitionPlacement -> verifyTimeDeviationFreshnessConstraint( specs, Arrays.asList( partitionPlacement ) ) );
        }

        return unfilteredPlacements;
    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnTimestampFreshness(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            Timestamp toleratedTimestampFilter ) {


        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {
            List<CatalogPartitionPlacement> partitionPlacements = entry.getValue();

            // Removes all Data Placements that are older than the toleratedTimestamp Value
            partitionPlacements.removeIf( partitionPlacement -> toleratedTimestampFilter.after( new Timestamp( partitionPlacement.updateInformation.commitTimestamp ) ) );
        }

        return unfilteredPlacements;

    }


    private Map<Long, List<CatalogPartitionPlacement>> filterOnFreshnessIndex(
            Map<Long, List<CatalogPartitionPlacement>> unfilteredPlacements,
            FreshnessSpecification specs ) {

        for ( Entry<Long, List<CatalogPartitionPlacement>> entry : unfilteredPlacements.entrySet() ) {
            List<CatalogPartitionPlacement> partitionPlacements = entry.getValue();
            partitionPlacements.removeIf( partitionPlacement -> verifyModificationDeviationFreshnessConstraint( specs, Arrays.asList( partitionPlacement ) ) );
        }

        return unfilteredPlacements;
    }

}

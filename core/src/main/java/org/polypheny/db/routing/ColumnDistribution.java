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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.util.Pair;


public final class ColumnDistribution implements FieldDistribution {

    private final List<Long> targetColumns;

    private final List<Long> targetPartitions;

    private final List<Long> sourcePartitions;
    private final List<Long> excludedAllocations;
    private final Snapshot snapshot;

    private final List<AllocationEntity> allocations;

    private final Map<Long, List<AllocationColumn>> placementColumns;

    @Getter
    private final List<AllocationPartition> partitions;

    private final List<AllocationPlacement> placements;
    private final long logicalEntityId;

    @Getter
    private final LogicalTable table;

    @Getter
    private final List<LogicalColumn> primaryColumns;

    @Getter
    private final List<Long> primaryIds;

    private final Map<Long, AllocationEntity> fullPlacementsPerPartition = new HashMap<>();
    private final Map<Long, List<AllocationColumn>> fullColumnsPerPartition = new HashMap<>();

    private final MergeStrategy mergeStrategy;


    public ColumnDistribution( long logicalEntityId, List<Long> targetColumns, List<Long> targetPartitions, List<Long> sourcePartitions, List<Long> excludedAllocations, Snapshot snapshot ) {
        this( logicalEntityId, targetColumns, targetPartitions, sourcePartitions, excludedAllocations, snapshot, new ShrinkingMergeStrategy() );
    }


    public ColumnDistribution( long logicalEntityId, List<Long> targetColumns, List<Long> targetPartitions, List<Long> sourcePartitions, List<Long> excludedAllocations, Snapshot snapshot, @NotNull MergeStrategy mergeStrategy ) {
        this.logicalEntityId = logicalEntityId;

        this.targetColumns = targetColumns;
        this.targetPartitions = targetPartitions;
        this.sourcePartitions = sourcePartitions;
        this.excludedAllocations = excludedAllocations;
        this.snapshot = snapshot;

        this.table = snapshot.rel().getTable( logicalEntityId ).orElseThrow();
        this.primaryColumns = getPrimaryColumns( snapshot );
        this.primaryIds = primaryColumns.stream().map( c -> c.id ).toList();
        this.placements = getPossiblePlacements();
        this.partitions = getPossiblePartitions();
        this.allocations = getPossibleAllocations();

        this.placementColumns = getPlacementColumns();

        targetPartitions.forEach( this::checkFullPlacement );
        this.mergeStrategy = mergeStrategy;

    }


    @NotNull
    private List<LogicalColumn> getPrimaryColumns( Snapshot snapshot ) {
        if ( table.getDataModel() != DataModel.RELATIONAL || table.primaryKey == null ) {
            return List.of();
        }
        return snapshot.rel().getPrimaryKey( table.primaryKey ).orElseThrow().fieldIds.stream().map( columnId -> snapshot.rel().getColumn( columnId ).orElseThrow() ).toList();
    }


    public boolean needsMergeStrategy() {
        return fullPlacementsPerPartition.size() != targetPartitions.size();
    }


    public ColumnDistribution withMergeStrategy( MergeStrategy mergeStrategy ) {
        return new ColumnDistribution( logicalEntityId, targetColumns, targetPartitions, sourcePartitions, excludedAllocations, snapshot, mergeStrategy );
    }


    private Map<Long, List<AllocationColumn>> getPlacementColumns() {
        List<Long> placementIds = placements.stream().map( p -> p.id ).toList();
        return snapshot.alloc().getColumns().stream().filter( c -> logicalEntityId == c.logicalTableId && placementIds.contains( c.placementId ) && targetColumns.contains( c.columnId ) ).collect( Collectors.groupingBy( c -> c.placementId ) );
    }


    private List<AllocationPartition> getPossiblePartitions() {
        List<AllocationPartition> partitions = new ArrayList<>();
        snapshot.alloc().getPartitions().stream().filter( p -> p.logicalEntityId == logicalEntityId && sourcePartitions.contains( p.id ) ).forEach( partitions::add );
        return partitions;
    }


    private List<AllocationPlacement> getPossiblePlacements() {
        List<AllocationPlacement> placements = new ArrayList<>();
        snapshot.alloc().getPlacements().stream().filter( p -> p.logicalEntityId == logicalEntityId ).forEach( placements::add );
        return placements;
    }


    private List<AllocationEntity> getPossibleAllocations() {
        List<AllocationEntity> entities = new ArrayList<>();
        List<Long> placementIds = placements.stream().map( p -> p.id ).toList();
        List<Long> partitionIds = partitions.stream().map( p -> p.id ).toList();
        List<AllocationEntity> allocs = snapshot.alloc().getAllocations();
        allocs.stream().filter(
                a -> a.logicalId == logicalEntityId
                        && placementIds.contains( a.placementId )
                        && partitionIds.contains( a.partitionId )
                        && !excludedAllocations.contains( a.id ) ).forEach( entities::add );

        return entities;
    }


    public RoutedDistribution route() {
        return mergeStrategy.pick( this );
    }


    private void checkFullPlacement( long partitionId ) {
        for ( Entry<Long, List<AllocationColumn>> entry : placementColumns.entrySet() ) {
            if ( entry.getValue().size() == targetColumns.size() ) {
                for ( AllocationEntity entity : allocations ) {
                    if ( entity.partitionId == partitionId && entity.placementId == entry.getKey() ) {
                        this.fullPlacementsPerPartition.put( partitionId, entity );
                        this.fullColumnsPerPartition.put( partitionId, entry.getValue() );
                    }
                }
            }
        }
    }


    public Pair<AllocationEntity, List<AllocationColumn>> getFullPlacement( long partitionId ) {
        if ( !hasFullPlacement( partitionId ) ) {
            throw new GenericRuntimeException( "No full placement available" );
        }

        return Pair.of( fullPlacementsPerPartition.get( partitionId ), fullColumnsPerPartition.get( partitionId ) );
    }


    public boolean hasFullPlacement( long partitionId ) {
        return fullPlacementsPerPartition.containsKey( partitionId );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        ColumnDistribution that = (ColumnDistribution) o;

        if ( logicalEntityId != that.logicalEntityId ) {
            return false;
        }
        if ( !Objects.equals( targetColumns, that.targetColumns ) ) {
            return false;
        }
        if ( !Objects.equals( targetPartitions, that.targetPartitions ) ) {
            return false;
        }
        if ( !Objects.equals( excludedAllocations, that.excludedAllocations ) ) {
            return false;
        }
        if ( !Objects.equals( allocations, that.allocations ) ) {
            return false;
        }
        if ( !Objects.equals( placementColumns, that.placementColumns ) ) {
            return false;
        }
        if ( !Objects.equals( partitions, that.partitions ) ) {
            return false;
        }
        if ( !Objects.equals( placements, that.placements ) ) {
            return false;
        }
        if ( !Objects.equals( table, that.table ) ) {
            return false;
        }
        if ( !Objects.equals( primaryColumns, that.primaryColumns ) ) {
            return false;
        }
        if ( !Objects.equals( primaryIds, that.primaryIds ) ) {
            return false;
        }
        if ( !fullPlacementsPerPartition.equals( that.fullPlacementsPerPartition ) ) {
            return false;
        }
        if ( !fullColumnsPerPartition.equals( that.fullColumnsPerPartition ) ) {
            return false;
        }
        return Objects.equals( mergeStrategy, that.mergeStrategy );
    }


    @Override
    public int hashCode() {
        int result = targetColumns != null ? targetColumns.hashCode() : 0;
        result = 31 * result + (targetPartitions != null ? targetPartitions.hashCode() : 0);
        result = 31 * result + (excludedAllocations != null ? excludedAllocations.hashCode() : 0);
        result = 31 * result + (allocations != null ? allocations.hashCode() : 0);
        result = 31 * result + (placementColumns != null ? placementColumns.hashCode() : 0);
        result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
        result = 31 * result + (placements != null ? placements.hashCode() : 0);
        result = 31 * result + (int) (logicalEntityId ^ (logicalEntityId >>> 32));
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (primaryColumns != null ? primaryColumns.hashCode() : 0);
        result = 31 * result + (primaryIds != null ? primaryIds.hashCode() : 0);
        result = 31 * result + fullPlacementsPerPartition.hashCode();
        result = 31 * result + fullColumnsPerPartition.hashCode();
        result = 31 * result + (mergeStrategy != null ? mergeStrategy.hashCode() : 0);
        return result;
    }


    @Getter
    public abstract static class MergeStrategy {

        public RoutedDistribution pick( ColumnDistribution columnDistribution ) {
            List<FullPartition> partitions = new ArrayList<>();
            for ( long partition : columnDistribution.sourcePartitions ) {
                if ( columnDistribution.fullPlacementsPerPartition.containsKey( partition ) ) {
                    // have full placement for this partition
                    partitions.add( new FullPartition( partition, List.of( new PartialPartition( columnDistribution.fullPlacementsPerPartition.get( partition ), columnDistribution.fullColumnsPerPartition.get( partition ) ) ) ) );
                    continue;
                }

                partitions.add( pickPartition( partition, columnDistribution ) );

            }
            return new RoutedDistribution( columnDistribution.table, partitions );
        }


        abstract FullPartition pickPartition( long partitionId, ColumnDistribution columnDistribution );

    }


    // default merge strategy, which merges largest partitions first
    public static class ShrinkingMergeStrategy extends MergeStrategy {


        @Override
        FullPartition pickPartition( long partitionId, ColumnDistribution columnDistribution ) {
            return new FullPartition( partitionId, pick( columnDistribution, partitionId, columnDistribution.targetColumns, columnDistribution.placementColumns ) );
        }


        private List<PartialPartition> pick( ColumnDistribution columnDistribution, long partitionId, List<Long> requiredColumns, Map<Long, List<AllocationColumn>> placementsColumns ) {
            if ( requiredColumns.isEmpty() ) {
                return List.of();
            }

            Pair<Long, List<AllocationColumn>> longestRelevantPlacementColumns = null;
            for ( Entry<Long, List<AllocationColumn>> placementColumns : placementsColumns.entrySet() ) {
                if ( columnDistribution.allocations.stream().noneMatch( a -> a.partitionId == partitionId && a.placementId == placementColumns.getKey() ) ) {
                    // this placement does not exist for this partition
                    continue;
                }
                if ( longestRelevantPlacementColumns == null ) {
                    longestRelevantPlacementColumns = Pair.of( placementColumns.getKey(), placementColumns.getValue() );
                    continue;
                }
                long relevantCount = placementColumns.getValue().stream().map( c -> c.columnId ).filter( requiredColumns::contains ).count();
                if ( relevantCount > longestRelevantPlacementColumns.right.size() ) {
                    longestRelevantPlacementColumns = Pair.of( placementColumns.getKey(), placementColumns.getValue() );
                }
            }
            if ( longestRelevantPlacementColumns == null ) {
                throw new GenericRuntimeException( "No placement found for partition %s", partitionId );
            }

            List<PartialPartition> result = new ArrayList<>();
            long longestPlacement = longestRelevantPlacementColumns.getKey();
            List<AllocationColumn> longestColumns = longestRelevantPlacementColumns.right;
            // add currently longest placement
            result.add( new PartialPartition(
                    columnDistribution.allocations.stream().filter( a -> a.placementId == longestPlacement && a.partitionId == partitionId ).findAny().orElseThrow(),
                    longestRelevantPlacementColumns.right.stream().filter( c -> requiredColumns.contains( c.columnId ) || columnDistribution.primaryIds.contains( c.columnId ) ).toList() ) );

            Map<Long, List<AllocationColumn>> remaining = new HashMap<>( placementsColumns );
            remaining.remove( longestPlacement );

            List<Long> usedColumnIds = longestColumns.stream().map( ac -> ac.columnId ).toList();
            // search for next longest allocation
            result.addAll( pick( columnDistribution, partitionId, requiredColumns.stream().filter( c -> !usedColumnIds.contains( c ) ).toList(), remaining ) );

            return result;

        }

    }


    public record PartialPartition(AllocationEntity entity, List<AllocationColumn> columns) {

    }


    /**
     * <pre><code>
     *
     *
     *     partialPartition | partialPartition
     *     alloc + columns  | alloc + columns
     *    -------------------------------------
     *
     * </code></pre>
     *
     *
     * @param partials
     */
    public record FullPartition(long id, List<PartialPartition> partials) implements FieldDistribution {

        public boolean needsJoin() {
            return partials.size() > 1;
        }


        public List<AllocationColumn> getOrderedColumns() {
            List<AllocationColumn> columns = new ArrayList<>();
            for ( PartialPartition partial : partials ) {
                for ( AllocationColumn column : partial.columns ) {
                    if ( columns.stream().noneMatch( c -> c.columnId == column.columnId ) ) {
                        columns.add( column );
                    }
                }

            }
            columns.sort( ( a, b ) -> (int) (a.columnId - b.columnId) );
            return columns;
        }

    }


    public record RoutedDistribution(LogicalEntity entity, List<FullPartition> partitions) {

        public boolean needsUnion() {
            return partitions.size() > 1;
        }


        public boolean needsJoin() {
            return partitions.stream().anyMatch( FullPartition::needsJoin );
        }

    }

}

/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.catalog.entity.mapping;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;
import org.polypheny.db.util.Pair;

/**
 * Mapping lifecycle logic.
 */
public final class SchemaMappingManager {

    private SchemaMappingManager() {
    }


    /**
     * Creates a direct mapping object. The caller registers it after the adapter has registered its physical columns.
     */
    public static SchemaMapping createDefaultMapping( String adapterName, String sourceTableName, long logicalEntityId, DataModel dataModel, List<Long> columnIds ) {

        long id = IdBuilder.getInstance().getNewMappingId();
        String name = defaultMappingName( adapterName, sourceTableName );
        List<MappingRule> rules = columnIds.stream().<MappingRule>map( DirectColumnMappingRule::new ).toList();

        return new SchemaMapping( id, name, MappingKind.PHYSICAL_TO_LOGICAL, null, logicalEntityId, dataModel, dataModel, rules );
    }


    public static String defaultMappingName( String adapterName, String sourceTableName ) {
        return adapterName + "_" + sourceTableName + "_mapping";
    }


    /**
     * Returns a copy with the new label, preserving the mapping's identity, endpoints, models, and rules.
     */
    public static SchemaMapping renameMapping( SchemaMapping mapping, String name ) {
        return new SchemaMapping( mapping.id(), name, mapping.kind(), mapping.sourceEntityId(),
                mapping.targetEntityId(), mapping.sourceModel(), mapping.targetModel(), mapping.rules() );
    }


    /**
     * Returns the IDs of mappings whose target entity belongs to the given set of provided logical Entity Ids. The callers perform the deletion.
     */
    public static List<Long> getMappingIdsForEntities( Collection<SchemaMapping> mappings, Set<Long> logicalEntityIds ) {
        // it checks only the target
        return mappings.stream()
                .filter( mapping -> logicalEntityIds.contains( mapping.targetEntityId() ) )
                .map( SchemaMapping::id )
                .toList();
    }


    /**
     * Determines which logical columns should be hidden because mappings omit them,
     * and rejects exclusions that would break foreign keys or views.
     */
    public static Set<Long> getExcludedColumnIds( MappingSnapshot mappings, LogicalRelSnapshot rels ) {
        Set<Long> excluded = new HashSet<>();
        for ( SchemaMapping mapping : mappings.getMappings() ) {
            LogicalTable table = rels.getTable( mapping.targetEntityId() ).orElseThrow();
            if ( table.entityType == EntityType.SOURCE && !table.modifiable ) {
                Set<Long> included = mapping.rules().stream().filter( DirectColumnMappingRule.class::isInstance )
                        .map( DirectColumnMappingRule.class::cast ).map( DirectColumnMappingRule::targetLogicalColumnId ).collect( Collectors.toSet() );
                rels.getColumns( table.id ).stream().map( c -> c.id ).filter( c -> !included.contains( c ) ).forEach( excluded::add );
            }
        }
        if ( !excluded.isEmpty() ) {
            for ( var key : rels.getForeignKeys() ) {
                if ( key.fieldIds.stream().anyMatch( excluded::contains ) || key.referencedKeyFieldIds.stream().anyMatch( excluded::contains ) ) {
                    throw new GenericRuntimeException( "Mapping excludes a column required by foreign key %s.", key.name );
                }
            }
            for ( long tableId : mappings.getMappings().stream().map( SchemaMapping::targetEntityId ).toList() ) {
                for ( var view : rels.getConnectedViews( tableId ) ) {
                    if ( view.underlyingTables.getOrDefault( tableId, List.of() ).stream().anyMatch( excluded::contains ) ) {
                        throw new GenericRuntimeException( "Mapping excludes a column required by view %s.", view.name );
                    }
                }
            }
        }
        return Set.copyOf( excluded );
    }


    /**
     * Checks each mapping before a snapshot is published.
     * Validation uses all accepted columns, before source mappings filter the published schema.
     *
     * @param mappings mapping definitions to validate
     * @param rels unfiltered relational metadata
     * @param alloc unfiltered allocation metadata
     * @param catalog adapter catalog lookup
     */
    public static void validateMappings( MappingSnapshot mappings, LogicalRelSnapshot rels, AllocSnapshot alloc, Catalog catalog ) {
        for ( SchemaMapping mapping : mappings.getMappings() ) {
            validateMappingKind( mapping );
            LogicalTable logicalTable = validateTargetTable( mapping, rels );
            List<AllocationEntity> allocations = validateAllocationPresence( mapping, alloc );
            Map<Long, LogicalColumn> logicalColumns = rels.getColumns( mapping.targetEntityId() ).stream().collect( Collectors.toMap( column -> column.id, column -> column ) );
            Set<Long> mappedColumns = validateLogicalColumnRules( mapping, logicalColumns );
            validateLogicalColumnCoverage( mapping, logicalTable, logicalColumns, mappedColumns );
            Map<Long, Set<Long>> physicalColumnsByPartition = validatePhysicalAllocations( mapping, logicalTable, logicalColumns, allocations, alloc, catalog );
            validatePartitionCoverage( mapping, allocations, physicalColumnsByPartition );
        }
    }


    private static void validateMappingKind( SchemaMapping mapping ) {
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL ) {
            throw new GenericRuntimeException( "Mapping %d has unsupported kind %s; only physical-to-logical mappings can currently be published.", mapping.id(), mapping.kind() );
        }
    }


    private static LogicalTable validateTargetTable( SchemaMapping mapping, LogicalRelSnapshot rels ) {
        LogicalTable logicalTable = rels.getTable( mapping.targetEntityId() ).orElse( null );
        if ( logicalTable == null ) {
            throw new GenericRuntimeException( "Mapping %d references missing relational entity %d.", mapping.id(), mapping.targetEntityId() );
        }
        if ( mapping.targetModel() != logicalTable.dataModel ) {
            throw new GenericRuntimeException( "Mapping %d target model %s does not match entity %d's model %s.",
                    mapping.id(), mapping.targetModel(), logicalTable.id, logicalTable.dataModel );
        }
        return logicalTable;
    }


    private static List<AllocationEntity> validateAllocationPresence( SchemaMapping mapping, AllocSnapshot alloc ) {
        List<AllocationEntity> allocations = alloc.getFromLogical( mapping.targetEntityId() );
        if ( allocations.isEmpty() ) {
            throw new GenericRuntimeException( "Mapping %d requires an allocation for logical entity %d.", mapping.id(), mapping.targetEntityId() );
        }
        return allocations;
    }


    private static Set<Long> validateLogicalColumnRules( SchemaMapping mapping, Map<Long, LogicalColumn> logicalColumns ) {
        Set<Long> mappedColumns = new HashSet<>();
        Set<Integer> mappedPositions = new HashSet<>();
        for ( var rule : mapping.rules() ) {
            if ( rule instanceof DirectColumnMappingRule direct ) {
                long columnId = direct.targetLogicalColumnId();
                LogicalColumn logicalColumn = logicalColumns.get( columnId );
                if ( logicalColumn == null ) {
                    throw new GenericRuntimeException( "Mapping %d references missing logical column %d.", mapping.id(), columnId );
                }
                mappedColumns.add( columnId );
                if ( !mappedPositions.add( logicalColumn.position ) ) {
                    throw new GenericRuntimeException( "Mapping %d has duplicate logical column position %d.", mapping.id(), logicalColumn.position );
                }
            }
        }
        if ( mappedColumns.isEmpty() ) {
            throw new GenericRuntimeException( "Mapping %d must include at least one logical column.", mapping.id() );
        }
        return mappedColumns;
    }


    private static void validateLogicalColumnCoverage( SchemaMapping mapping, LogicalTable logicalTable, Map<Long, LogicalColumn> logicalColumns, Set<Long> mappedColumns ) {
        for ( LogicalColumn logicalColumn : logicalColumns.values() ) {
            if ( !mappedColumns.contains( logicalColumn.id ) && (logicalTable.entityType != EntityType.SOURCE || logicalTable.modifiable) ) {
                throw new GenericRuntimeException( "Mapping %d has no rule for logical column %d.", mapping.id(), logicalColumn.id );
            }
        }
    }


    private static Map<Long, Set<Long>> validatePhysicalAllocations( SchemaMapping mapping, LogicalTable logicalTable, Map<Long, LogicalColumn> logicalColumns,
            List<AllocationEntity> allocations, AllocSnapshot alloc, Catalog catalog ) {
        Map<Long, Set<Long>> physicalColumnsByPartition = new HashMap<>();
        for ( AllocationEntity allocation : allocations ) {
            AdapterCatalog adapter = validateAdapterAllocation( mapping, allocation, catalog );
            PhysicalTable table = validatePhysicalTable( mapping, logicalTable, allocation, adapter );
            Map<Long, PhysicalColumn> tableColumns = validatePhysicalColumnUniqueness( mapping, allocation, table );
            Set<Long> allocatedColumns = validateAllocationColumns( mapping, logicalColumns, allocation, alloc, adapter, tableColumns );
            if ( !allocatedColumns.isEmpty() ) {
                physicalColumnsByPartition.computeIfAbsent( allocation.partitionId, ignored -> new HashSet<>() ).addAll( allocatedColumns );
            }
        }
        return physicalColumnsByPartition;
    }


    private static AdapterCatalog validateAdapterAllocation( SchemaMapping mapping, AllocationEntity allocation, Catalog catalog ) {
        AdapterCatalog adapter = catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow( () ->
                new GenericRuntimeException( "Mapping %d has no adapter catalog for allocation %d.", mapping.id(), allocation.id ) );
        if ( !(adapter instanceof RelAdapterCatalog) ) {
            throw new GenericRuntimeException( "Mapping %d requires a relational adapter catalog for allocation %d.", mapping.id(), allocation.id );
        }
        AllocationEntity registeredAllocation = adapter.getAlloc( allocation.id );
        if ( registeredAllocation == null || registeredAllocation.logicalId != mapping.targetEntityId()
                || registeredAllocation.adapterId != allocation.adapterId || registeredAllocation.partitionId != allocation.partitionId
                || registeredAllocation.placementId != allocation.placementId ) {
            throw new GenericRuntimeException( "Mapping %d has no adapter-owned allocation %d.", mapping.id(), allocation.id );
        }
        return adapter;
    }


    private static PhysicalTable validatePhysicalTable( SchemaMapping mapping, LogicalTable logicalTable, AllocationEntity allocation, AdapterCatalog adapter ) {
        List<PhysicalEntity> physicals = adapter.getPhysicalsFromAllocs( allocation.id );
        if ( physicals == null || physicals.isEmpty() || !(physicals.get( 0 ) instanceof PhysicalTable table)
                || table.allocationId != allocation.id || table.logicalId != mapping.targetEntityId() || table.adapterId != allocation.adapterId
                || table.namespaceId != logicalTable.namespaceId ) {
            throw new GenericRuntimeException( "Mapping %d has no matching physical table for allocation %d.", mapping.id(), allocation.id );
        }
        if ( mapping.sourceModel() != table.dataModel ) {
            throw new GenericRuntimeException( "Mapping %d source model %s does not match allocation %d's physical model %s.",
                    mapping.id(), mapping.sourceModel(), allocation.id, table.dataModel );
        }
        return table;
    }


    private static Map<Long, PhysicalColumn> validatePhysicalColumnUniqueness( SchemaMapping mapping, AllocationEntity allocation, PhysicalTable table ) {
        Map<Long, PhysicalColumn> tableColumns = new HashMap<>();
        Set<Integer> physicalPositions = new HashSet<>();
        for ( PhysicalColumn column : table.columns ) {
            if ( tableColumns.putIfAbsent( column.id, column ) != null ) {
                throw new GenericRuntimeException( "Mapping %d has duplicate physical column %d in allocation %d.", mapping.id(), column.id, allocation.id );
            }
            if ( !physicalPositions.add( column.position ) ) {
                throw new GenericRuntimeException( "Mapping %d has duplicate physical column position %d in allocation %d.", mapping.id(), column.position, allocation.id );
            }
        }
        return tableColumns;
    }


    private static Set<Long> validateAllocationColumns( SchemaMapping mapping, Map<Long, LogicalColumn> logicalColumns, AllocationEntity allocation,
            AllocSnapshot alloc, AdapterCatalog adapter, Map<Long, PhysicalColumn> tableColumns ) {
        Set<Long> allocatedColumns = new HashSet<>();
        for ( AllocationColumn allocatedColumn : alloc.getColumns( allocation.placementId ) ) {
            long columnId = allocatedColumn.columnId;
            if ( allocatedColumn.logicalTableId != mapping.targetEntityId() || allocatedColumn.adapterId != allocation.adapterId ) {
                throw new GenericRuntimeException( "Mapping %d has mismatched column placement %d for allocation %d.", mapping.id(), columnId, allocation.id );
            }
            LogicalColumn logicalColumn = logicalColumns.get( columnId );
            if ( logicalColumn == null ) {
                throw new GenericRuntimeException( "Mapping %d has an allocation for missing logical column %d.", mapping.id(), columnId );
            }
            validatePhysicalColumn( mapping, logicalColumn, allocation, adapter, tableColumns.get( columnId ) );
            allocatedColumns.add( columnId );
        }
        return allocatedColumns;
    }


    private static void validatePhysicalColumn( SchemaMapping mapping, LogicalColumn logicalColumn, AllocationEntity allocation, AdapterCatalog adapter, PhysicalColumn tableColumn ) {
        long columnId = logicalColumn.id;
        if ( !(adapter.fields.get( Pair.of( allocation.id, columnId ) ) instanceof PhysicalColumn physicalColumn)
                || !physicalColumn.equals( tableColumn )
                || physicalColumn.allocId != allocation.id || physicalColumn.logicalEntityId != mapping.targetEntityId()
                || physicalColumn.adapterId != allocation.adapterId ) {
            throw new GenericRuntimeException( "Mapping %d has no matching physical column %d for allocation %d.", mapping.id(), columnId, allocation.id );
        }
        if ( !hasMatchingType( logicalColumn, physicalColumn ) ) {
            throw new GenericRuntimeException( "Mapping %d has incompatible physical column %d in allocation %d.", mapping.id(), columnId, allocation.id );
        }
    }


    private static void validatePartitionCoverage( SchemaMapping mapping, List<AllocationEntity> allocations, Map<Long, Set<Long>> physicalColumnsByPartition ) {
        for ( AllocationEntity allocation : allocations ) {
            Set<Long> availableColumns = physicalColumnsByPartition.getOrDefault( allocation.partitionId, Set.of() );
            for ( var rule : mapping.rules() ) {
                if ( rule instanceof DirectColumnMappingRule direct && !availableColumns.contains( direct.targetLogicalColumnId() ) ) {
                    throw new GenericRuntimeException( "Mapping %d references logical column %d unavailable on partition %d.",
                            mapping.id(), direct.targetLogicalColumnId(), allocation.partitionId );
                }
            }
        }
    }


    private static boolean hasMatchingType( LogicalColumn logical, PhysicalColumn physical ) {
        return logical.type == physical.type
                && logical.collectionsType == physical.collectionsType
                && Objects.equals( logical.length, physical.length )
                && Objects.equals( logical.scale, physical.scale )
                && Objects.equals( logical.dimension, physical.dimension )
                && Objects.equals( logical.cardinality, physical.cardinality )
                && logical.nullable == physical.nullable
                && logical.collation == physical.collation;
    }
}

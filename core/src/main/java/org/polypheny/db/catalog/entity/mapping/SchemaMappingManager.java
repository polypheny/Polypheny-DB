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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.algebra.mapping.DocumentMappingBinding;
import org.polypheny.db.algebra.mapping.RuleRepository;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RelConvertColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RelCopyColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentCopyEntireMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RuleDefinition;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.RelMappingUpdate.ColumnName;
import org.polypheny.db.util.Pair;

/**
 * Mapping lifecycle logic.
 */
public final class SchemaMappingManager {

    public static final int MAPPING_NAME_MAX_LENGTH = 255;
    public static final int IDENTIFIER_MAX_LENGTH = 100;
    public static final String IDENTIFIER_PATTERN = "[a-zA-Z_][a-zA-Z0-9_]*";
    public static final String LAST_SOURCE_MAPPING_REASON = "An imported source table must always have a mapping. Remove the source table or adapter instead.";

    private SchemaMappingManager() {
    }


    public static String mappingNameMessage() {
        return "Enter a mapping name of 1–" + MAPPING_NAME_MAX_LENGTH + " characters.";
    }


    public static String identifierMessage() {
        return "Use an identifier of up to " + IDENTIFIER_MAX_LENGTH + " letters, digits or underscores, starting with a letter or underscore.";
    }


    /**
     * Creates a direct mapping object. The caller registers it after the adapter has registered its physical columns.
     */
    public static SchemaMapping createDefaultMapping( String adapterName, String sourceTableName, LogicalTable table, List<Long> columnIds ) {
        return createDefaultMapping( adapterName, sourceTableName, table.id, table.dataModel, columnIds );
    }


    public static SchemaMapping createDefaultMapping( String adapterName, String sourceTableName, long logicalEntityId, DataModel dataModel, List<Long> columnIds ) {
        return createMapping( defaultMappingName( adapterName, sourceTableName ), logicalEntityId, dataModel, columnIds );
    }


    public static boolean isMappingNameTaken( Collection<SchemaMapping> mappings, String name, long mappingId ) {
        String key = mappingNameKey( name );
        return mappings.stream().anyMatch( other -> other.id() != mappingId && mappingNameKey( other.name() ).equals( key ) );
    }


    public record PhysicalSource( long adapterId, String namespaceName, String tableName ) {
    }


    public static List<AllocationEntity> sourceAllocations( Catalog catalog, SchemaMapping mapping ) {
        return catalog.getSnapshot().alloc().getFromLogical( mapping.targetEntityId() ).stream()
                .filter( allocation -> mapping.sourceAllocationId() == null || mapping.sourceAllocationId() == allocation.id ).toList();
    }

    public static Set<PhysicalSource> physicalSources( Catalog catalog, SchemaMapping mapping ) {
        return sourceAllocations( catalog, mapping ).stream()
                .flatMap( allocation -> catalog.getAdapterCatalog( allocation.adapterId )
                        .orElseThrow( () -> MappingError.Missing.adapterCatalog( allocation.adapterId ) )
                        .getPhysicalsFromAllocs( allocation.id ).stream() )
                .map( physical -> new PhysicalSource( physical.adapterId, physical.namespaceName, physical.name ) ).collect( Collectors.toSet() );
    }


    public static boolean isPhysicalSourceMappedToTarget( Catalog catalog, SchemaMapping source, long targetEntityId ) {
        Set<PhysicalSource> sources = physicalSources( catalog, source );
        return catalog.getSnapshot().mappings().getMappingsForLogicalEntity( targetEntityId ).stream()
                .flatMap( mapping -> physicalSources( catalog, mapping ).stream() ).anyMatch( sources::contains );
    }

    public static String removalBlockedReason( Catalog catalog, SchemaMapping mapping ) {
        return switch ( mapping.targetModel() ) {
            case RELATIONAL -> relRemovalBlockedReason( catalog, mapping );
            case DOCUMENT -> documentRemovalBlockedReason( catalog, mapping );
            case GRAPH -> throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
        };
    }


    private static String relRemovalBlockedReason( Catalog catalog, SchemaMapping mapping ) {
        Snapshot snapshot = catalog.getSnapshot();
        var mappings = snapshot.mappings().getMappings();
        var table = snapshot.rel().getTable( mapping.targetEntityId() ).orElse( null );
        if ( mappings.stream().anyMatch( other -> other.sourceEntityId() != null && other.sourceEntityId() == mapping.targetEntityId() ) ) {
            return "The target table is used as a source by another mapping.";
        }
        if ( table != null && table.entityType == EntityType.SOURCE && mapping.kind() == MappingKind.PHYSICAL_TO_LOGICAL ) {
            Set<PhysicalSource> remainingSources = mappings.stream()
                    .filter( other -> other.id() != mapping.id() && other.kind() == MappingKind.PHYSICAL_TO_LOGICAL )
                    .flatMap( other -> physicalSources( catalog, other ).stream() ).collect( Collectors.toSet() );
            if ( !remainingSources.containsAll( physicalSources( catalog, mapping ) ) ) {
                return LAST_SOURCE_MAPPING_REASON;
            }
        }
        return null;
    }


    private static String documentRemovalBlockedReason( Catalog catalog, SchemaMapping mapping ) {
        Snapshot snapshot = catalog.getSnapshot();
        var collection = snapshot.doc().getCollection( mapping.targetEntityId() )
                .orElseThrow( () -> MappingError.Missing.collection( mapping.targetEntityId() ) );
        var allocations = sourceAllocations( catalog, mapping );
        if ( allocations.size() != 1 ) {
            throw MappingError.Algebra.sourceBindingsAmbiguous();
        }
        long adapterId = allocations.get( 0 ).adapterId;
        var adapter = snapshot.getAdapter( adapterId ).orElseThrow( () -> MappingError.Missing.adapter( adapterId ) );
        String readOnlyReason = documentReadOnlyReason( adapter.type, collection );
        if ( readOnlyReason != null ) {
            return readOnlyReason;
        }
        if ( snapshot.mappings().getMappingsForLogicalEntity( collection.id ).size() == 1 && !documentDependencies( snapshot, collection.id ).isEmpty() ) {
            return "Dependent objects prevent dropping this mapping's logical collection.";
        }
        var sources = physicalSources( catalog, mapping );
        var adapterCatalog = catalog.getAdapterCatalog( adapterId ).orElseThrow( () -> MappingError.Missing.adapterCatalog( adapterId ) );
        // Legacy imported collections can still own a physical source without an explicit mapping.
        boolean otherTarget = adapterCatalog.physicals.values().stream()
                .filter( p -> p.dataModel == DataModel.DOCUMENT && p.logicalId != collection.id )
                .filter( p -> sources.contains( new PhysicalSource( p.adapterId, p.namespaceName, p.name ) ) )
                .anyMatch( p -> snapshot.doc().getCollection( p.logicalId ).filter( c -> c.entityType == EntityType.SOURCE && !c.modifiable ).isPresent() );
        return otherTarget ? null : "An imported source collection must always have a logical mapping. Remove the source collection or adapter instead.";
    }


    public static String documentReadOnlyReason( AdapterType adapterType, LogicalCollection collection ) {
        return adapterType != AdapterType.SOURCE || collection.entityType != EntityType.SOURCE || collection.modifiable
                ? "Only read-only imported document source collections can be edited here." : null;
    }


    public static List<String> documentDependencies( Snapshot snapshot, long collectionId ) {
        List<String> dependencies = new ArrayList<>();
        snapshot.mappings().getMappings().stream().filter( m -> Objects.equals( m.sourceEntityId(), collectionId ) )
                .map( SchemaMapping::name ).forEach( dependencies::add );
        snapshot.doc().getCollections( null, null ).stream().filter( c -> Objects.equals( c.synchronizedSourceEntityId, collectionId ) )
                .map( c -> c.name ).forEach( dependencies::add );
        snapshot.rel().getTables( (Pattern) null, null ).stream().filter( LogicalView.class::isInstance ).map( LogicalView.class::cast )
                .filter( v -> v.underlyingTables.containsKey( collectionId ) ).map( v -> v.name ).forEach( dependencies::add );
        return List.copyOf( dependencies );
    }


    private static String mappingNameKey( String name ) {
        return name.trim().toLowerCase( Locale.ROOT );
    }


    public static SchemaMapping createMapping( String name, long logicalEntityId, DataModel dataModel, List<Long> columnIds ) {
        long id = IdBuilder.getInstance().getNewMappingId();
        List<MappingRule> rules = dataModel == DataModel.DOCUMENT ? List.of( new DocumentCopyEntireMappingRule() )
                : columnIds.stream().map( SchemaMappingManager::defaultColumnRule ).toList();

        return new SchemaMapping( id, name, MappingKind.PHYSICAL_TO_LOGICAL, null, logicalEntityId, dataModel, dataModel, rules, null );
    }


    public static SchemaMapping refreshColumnRules( SchemaMapping mapping, List<LogicalColumn> currentColumns, Set<Long> previousColumnIds ) {
        Set<Long> currentColumnIds = currentColumns.stream().map( column -> column.id ).collect( Collectors.toSet() );
        List<MappingRule> rules = new ArrayList<>( mapping.rules().stream()
                .filter( rule -> retainRuleForColumns( rule, currentColumnIds ) ).toList() );
        for ( LogicalColumn column : currentColumns ) {
            if ( !previousColumnIds.contains( column.id ) ) {
                rules.add( defaultColumnRule( column.id ) );
            }
        }
        var excluded = new HashMap<>( mapping.excludedColumnRules() );
        excluded.keySet().retainAll( currentColumnIds );
        return withRules( mapping, rules, mapping.sourceAllocationId(), excluded );
    }


    public static SchemaMapping updateColumnRules( Catalog catalog, SchemaMapping mapping, List<ColumnName> updates,
            Map<Long, LogicalColumn> targetColumns, List<PhysicalTable> physicalTables ) {
        Map<Long, ColumnMappingRule> previousRules = mapping.rules().stream()
                .filter( ColumnMappingRule.class::isInstance ).map( ColumnMappingRule.class::cast )
                .collect( Collectors.toMap( ColumnMappingRule::targetLogicalColumnId, rule -> rule ) );
        Map<Long, String> excluded = new HashMap<>( mapping.excludedColumnRules() );
        Set<Long> retained = new HashSet<>();
        List<ColumnName> toRebuild = new ArrayList<>();
        for ( ColumnName update : updates ) {
            long columnId = update.logicalColumnId();
            ColumnMappingRule previous = previousRules.get( columnId );
            if ( !update.included() ) {
                if ( previous != null ) {
                    RuleRepository.definitionFor( previous.getClass() ).ifPresent( rule -> excluded.put( columnId, rule.id() ) );
                }
                continue;
            }
            if ( update.targetType() != null || previous == null
                    || update.ruleType() != null && !hasRuleType( previous, update.ruleType() ) ) {
                toRebuild.add( update );
            } else {
                retained.add( columnId );
            }
        }

        List<MappingRule> rules = new ArrayList<>( mapping.rules().stream().filter( rule -> retainRuleForColumns( rule, retained ) ).toList() );
        for ( ColumnName update : toRebuild ) {
            String lastRule = excluded.remove( update.logicalColumnId() );
            String ruleType = update.ruleType() != null ? update.ruleType() : lastRule;
            rules.add( restoredColumnRule( targetColumns.get( update.logicalColumnId() ), physicalTables, ruleType ) );
        }

        Long allocationId = mapping.sourceAllocationId();
        if ( allocationId == null && requiresExplicitSourceAllocation( rules, targetColumns, physicalTables.stream().flatMap( table -> table.columns.stream() ) ) ) {
            allocationId = sourceAllocations( catalog, mapping ).get( 0 ).id;
        }
        return withRules( mapping, rules, allocationId, excluded );
    }


    private static boolean hasRuleType( MappingRule rule, String id ) {
        return RuleRepository.definitionFor( rule.getClass() ).map( RuleDefinition::id ).filter( id::equals ).isPresent();
    }


    private static boolean retainRuleForColumns( MappingRule rule, Set<Long> columnIds ) {
        return !(rule instanceof ColumnMappingRule columnRule) || columnIds.contains( columnRule.targetLogicalColumnId() );
    }


    public static MappingRule defaultColumnRule( long columnId ) {
        return new RelCopyColumnMappingRule( columnId );
    }


    /**
     * Rebuilds a rule when we re-include a previously excluded column or update its type or nullability.
     */
    private static MappingRule restoredColumnRule( LogicalColumn target, List<PhysicalTable> physicalTables, String ruleType ) {
        // Excluding the column removes its rule, so updateRelMapping needs to reconstruct it when the column is included again
        Stream<PhysicalColumn> physicalColumns = physicalTables.stream().flatMap( table -> table.columns.stream() )
                .filter( column -> column.id == target.id );
        return chooseColumnRule( target, physicalColumns, ruleType );
    }


    /**
     * Create appropriate mapping rule according to if conversion needed
     */
    public static MappingRule chooseColumnRule( LogicalColumn target, Stream<PhysicalColumn> physicalColumns, String ruleType ) {
        if ( RelCopyColumnMappingRule.DEFINITION.id().equals( ruleType ) ) {
            return defaultColumnRule( target.id );
        }
        if ( RelConvertColumnMappingRule.DEFINITION.id().equals( ruleType ) ) {
            return new RelConvertColumnMappingRule( target.id );
        }
        if ( ruleType != null ) {
            throw MappingError.Unsupported.columnRule( ruleType );
        }
        boolean requiresConversion = physicalColumns.anyMatch( column -> !hasMatchingValueType( target, column ) );
        return requiresConversion
                ? new RelConvertColumnMappingRule( target.id ) // when the validated request changes the type
                : defaultColumnRule( target.id ); // otherwise
    }


    public static boolean requiresExplicitSourceAllocation( List<MappingRule> rules, Map<Long, LogicalColumn> targets, Stream<PhysicalColumn> sources ) {
        return SchemaMapping.hasRule( rules, RelConvertColumnMappingRule.class ) || sources.anyMatch( source -> {
            LogicalColumn target = targets.get( source.id );
            return target != null && target.nullable != source.nullable;
        } );
    }


    public static boolean preservesTargetSchemaOnRefresh( SchemaMapping mapping ) {
        return mapping.sourceAllocationId() != null || mapping.hasRule( RelConvertColumnMappingRule.class );
    }


    public static boolean usesColumnTransformations( Snapshot snapshot, SchemaMapping mapping ) {
        if ( mapping.sourceModel() != DataModel.RELATIONAL || mapping.targetModel() != DataModel.RELATIONAL ) {
            return false;
        }
        if ( mapping.hasRule( RelConvertColumnMappingRule.class ) ) {
            return true;
        }
        if ( mapping.sourceAllocationId() == null ) {
            return false;
        }
        Set<Long> included = mappedColumnIds( mapping );
        return snapshot.alloc().getFromLogical( mapping.targetEntityId() ).stream()
                .filter( allocation -> allocation.id == mapping.sourceAllocationId() ).findFirst()
                .flatMap( allocation -> Catalog.getInstance().getAdapterCatalog( allocation.adapterId )
                        .map( catalog -> catalog.getPhysicalsFromAllocs( allocation.id ) ) )
                .stream().flatMap( Collection::stream )
                .map( physical -> physical.unwrap( PhysicalTable.class ) ).flatMap( Optional::stream )
                .flatMap( table -> table.columns.stream() )
                .anyMatch( column -> included.contains( column.id ) && snapshot.rel().getColumn( column.id )
                        .filter( target -> target.nullable != column.nullable ).isPresent() );
    }


    private static SchemaMapping withRules( SchemaMapping mapping, List<MappingRule> rules, Long allocationId, Map<Long, String> excluded ) {
        return new SchemaMapping( mapping.id(), mapping.name(), mapping.kind(), mapping.sourceEntityId(),
                mapping.targetEntityId(), mapping.sourceModel(), mapping.targetModel(), rules, allocationId, excluded );
    }


    public static void validateRefreshedSourceColumns( String targetName, SchemaMapping mapping, LogicalRelSnapshot rels,
            Map<Long, PhysicalColumn> previousColumns, Map<String, ExportedColumn> exportedByName,
            BiPredicate<LogicalColumn, ExportedColumn> matchesSourceType ) {
        for ( MappingRule rule : mapping.rules() ) {
            if ( !(rule instanceof ColumnMappingRule columnRule) ) {
                throw MappingError.Unsupported.refreshRule( mapping.name() );
            }
            long targetId = columnRule.targetLogicalColumnId();
            LogicalColumn target = rels.getColumn( targetId )
                    .orElseThrow( () -> MappingError.Missing.logicalColumn( mapping.id(), targetId ) );
            PhysicalColumn previous = previousColumns.get( targetId );
            ExportedColumn exported = previous == null ? null : exportedByName.get( previous.name.toLowerCase( Locale.ROOT ) );
            String sourceName = previous == null ? target.name : previous.name;
            if ( exported == null ) {
                throw MappingError.Missing.sourceColumn( sourceName, mapping.name(), targetName, target.name );
            }
            if ( !isCompatibleRefreshedSourceColumn( columnRule, target, exported, matchesSourceType ) ) {
                throw MappingError.Incompatible.sourceColumn( sourceName, mapping.name(), targetName, target.name );
            }
        }
    }


    private static boolean isCompatibleRefreshedSourceColumn( ColumnMappingRule rule, LogicalColumn target, ExportedColumn exported,
            BiPredicate<LogicalColumn, ExportedColumn> matchesSourceType ) {
        if ( rule instanceof RelConvertColumnMappingRule ) {
            AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
            AlgDataType sourceType = LogicalColumn.getAlgDataType( factory, exported.length(), exported.scale(), exported.type(),
                    exported.collectionsType(), exported.cardinality(), exported.dimension(), exported.nullable(), exported.elementsNullable() );
            return RelConvertColumnMappingRule.supports( sourceType, target.getAlgDataType( factory ) );
        }
        return matchesSourceType.test( target, exported ) && target.elementsNullable == exported.elementsNullable();
    }


    public static String defaultMappingName( String adapterName, String sourceTableName ) {
        return adapterName + "_" + sourceTableName + "_mapping";
    }


    public static String availableDefaultMappingName( Collection<SchemaMapping> mappings, String adapterName, String sourceTableName ) {
        String baseName = defaultMappingName( adapterName, sourceTableName );
        String name = baseName;
        for ( int suffix = 1; isMappingNameTaken( mappings, name, -1 ); suffix++ ) {
            name = baseName + "_" + suffix;
        }
        return name;
    }


    /**
     * Returns a copy with the new label, preserving the mapping's identity, endpoints, models, and rules.
     */
    public static SchemaMapping renameMapping( SchemaMapping mapping, String name ) {
        return new SchemaMapping( mapping.id(), name, mapping.kind(), mapping.sourceEntityId(),
                mapping.targetEntityId(), mapping.sourceModel(), mapping.targetModel(), mapping.rules(), mapping.sourceAllocationId(), mapping.excludedColumnRules() );
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
            if ( mapping.targetModel() != DataModel.RELATIONAL ) {
                continue;
            }
            LogicalTable table = rels.getTable( mapping.targetEntityId() )
                    .orElseThrow( () -> MappingError.Missing.target( mapping.id(), mapping.targetEntityId() ) );
            if ( table.entityType == EntityType.SOURCE && !table.modifiable ) {
                Set<Long> included = mappedColumnIds( mapping );
                rels.getColumns( table.id ).stream().map( c -> c.id ).filter( c -> !included.contains( c ) ).forEach( excluded::add );
            }
        }
        if ( !excluded.isEmpty() ) {
            for ( var key : rels.getForeignKeys() ) {
                if ( key.fieldIds.stream().anyMatch( excluded::contains ) || key.referencedKeyFieldIds.stream().anyMatch( excluded::contains ) ) {
                    throw MappingError.Conflict.foreignKeyColumnRequired( key.name );
                }
            }
            for ( long tableId : mappings.getMappings().stream().filter( m -> m.targetModel() == DataModel.RELATIONAL ).map( SchemaMapping::targetEntityId ).toList() ) {
                for ( var view : rels.getConnectedViews( tableId ) ) {
                    if ( view.underlyingTables.getOrDefault( tableId, List.of() ).stream().anyMatch( excluded::contains ) ) {
                        throw MappingError.Conflict.viewColumnRequired( view.name );
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
     * @param docs logical document metadata
     * @param alloc unfiltered allocation metadata
     * @param catalog adapter catalog lookup
     */
    public static void validateMappings( MappingSnapshot mappings, LogicalRelSnapshot rels, LogicalDocSnapshot docs, AllocSnapshot alloc, Catalog catalog ) {
        for ( SchemaMapping mapping : mappings.getMappings() ) {
            if ( mapping.sourceModel() != mapping.targetModel() ) {
                throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
            }
            switch ( mapping.targetModel() ) {
                case RELATIONAL -> validateRelMapping( mapping, mappings, rels, alloc, catalog );
                case DOCUMENT -> validateDocumentMapping( mapping, mappings, docs, alloc, catalog );
                case GRAPH -> throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
            }
        }
    }


    private static void validateRelMapping( SchemaMapping mapping, MappingSnapshot mappings, LogicalRelSnapshot rels, AllocSnapshot alloc, Catalog catalog ) {
        var siblings = mappings.getMappingsForLogicalEntity( mapping.targetEntityId() );
        if ( siblings.size() > 1 && mapping.sourceAllocationId() == null ) {
            throw MappingError.Missing.explicitSourceAllocation( mapping.id() );
        }
        if ( siblings.size() > 1 && siblings.stream().anyMatch( other -> other.id() != mapping.id()
                && Objects.equals( other.sourceAllocationId(), mapping.sourceAllocationId() ) ) ) {
            throw MappingError.Duplicate.sourceAllocation( mapping.id(), mapping.sourceAllocationId() );
        }
        if ( siblings.size() > 1 && siblings.stream().anyMatch( other -> !mappedColumnIds( other ).equals( mappedColumnIds( mapping ) ) ) ) {
            throw MappingError.Incompatible.sharedColumnCoverage();
        }
        if ( siblings.size() > 1 && alloc.getFromLogical( mapping.targetEntityId() ).stream().map( allocation -> allocation.partitionId ).distinct().count() != 1 ) {
            throw MappingError.Unsupported.singleTargetPartitionRequired();
        }
        validateMappingKind( mapping );
        LogicalTable logicalTable = validateTargetTable( mapping, rels );
        List<AllocationEntity> allocations = validateAllocationPresence( mapping, alloc );
        Map<Long, LogicalColumn> logicalColumns = rels.getColumns( mapping.targetEntityId() ).stream().collect( Collectors.toMap( column -> column.id, column -> column ) );
        Map<Long, ColumnMappingRule> columnRules = validateLogicalColumnRules( mapping, logicalColumns );
        validateLogicalColumnCoverage( mapping, logicalTable, logicalColumns, columnRules.keySet() );
        Map<Long, Set<Long>> physicalColumnsByPartition = validatePhysicalAllocations( mapping, logicalTable, logicalColumns, columnRules, allocations, alloc, catalog );
        validatePartitionCoverage( mapping, allocations, physicalColumnsByPartition );
    }


    private static void validateMappingKind( SchemaMapping mapping ) {
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL ) {
            throw MappingError.Unsupported.kind( mapping.id(), mapping.kind() );
        }
    }


    private static void validateDocumentMapping( SchemaMapping mapping, MappingSnapshot mappings, LogicalDocSnapshot docs, AllocSnapshot alloc, Catalog catalog ) {
        DocumentMappingBinding.validateIdentity( mapping );
        var collection = docs.getCollection( mapping.targetEntityId() ).orElseThrow( () -> MappingError.Missing.collection( mapping.targetEntityId() ) );
        var siblings = mappings.getMappingsForLogicalEntity( mapping.targetEntityId() );
        if ( siblings.size() > 1 ) {
            if ( mapping.sourceAllocationId() == null ) {
                throw MappingError.Missing.explicitSourceAllocation( mapping.id() );
            }
            if ( siblings.stream().anyMatch( other -> other.id() != mapping.id() && Objects.equals( other.sourceAllocationId(), mapping.sourceAllocationId() ) ) ) {
                throw MappingError.Duplicate.sourceAllocation( mapping.id(), mapping.sourceAllocationId() );
            }
            if ( alloc.getPartitionsFromLogical( collection.id ).size() != 1 ) {
                throw MappingError.Unsupported.singleTargetPartitionRequired();
            }
        }
        var allocations = validateAllocationPresence( mapping, alloc );
        if ( allocations.size() != 1 ) {
            throw MappingError.Algebra.sourceBindingsAmbiguous();
        }
        var allocation = allocations.get( 0 );
        if ( allocation.namespaceId != collection.namespaceId ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), allocation.id );
        }
        var placement = alloc.getPlacementsFromLogical( mapping.targetEntityId() ).stream()
                .filter( p -> p.id == allocation.placementId && p.adapterId == allocation.adapterId ).findFirst();
        if ( placement.isEmpty() || alloc.getPartitionsFromLogical( mapping.targetEntityId() ).stream().noneMatch( p -> p.id == allocation.partitionId ) ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), allocation.id );
        }
        DocumentMappingBinding.create( mapping, allocation, catalog.getAdapterCatalog( allocation.adapterId )
                .orElseThrow( () -> MappingError.Missing.adapterCatalog( allocation.adapterId ) ) );
    }


    private static Set<Long> mappedColumnIds( SchemaMapping mapping ) {
        return mapping.rules().stream().filter( ColumnMappingRule.class::isInstance ).map( ColumnMappingRule.class::cast )
                .map( ColumnMappingRule::targetLogicalColumnId ).collect( Collectors.toSet() );
    }


    private static LogicalTable validateTargetTable( SchemaMapping mapping, LogicalRelSnapshot rels ) {
        LogicalTable logicalTable = rels.getTable( mapping.targetEntityId() ).orElse( null );
        if ( logicalTable == null ) {
            throw MappingError.Missing.target( mapping.id(), mapping.targetEntityId() );
        }
        if ( mapping.targetModel() != logicalTable.dataModel ) {
            throw MappingError.Incompatible.targetModel( mapping.id(), mapping.targetModel(), logicalTable.id, logicalTable.dataModel );
        }
        return logicalTable;
    }


    private static List<AllocationEntity> validateAllocationPresence( SchemaMapping mapping, AllocSnapshot alloc ) {
        List<AllocationEntity> allocations = alloc.getFromLogical( mapping.targetEntityId() ).stream()
                .filter( allocation -> mapping.sourceAllocationId() == null || mapping.sourceAllocationId() == allocation.id ).toList();
        if ( allocations.isEmpty() ) {
            throw MappingError.Missing.allocation( mapping.id(), mapping.targetEntityId() );
        }
        return allocations;
    }


    private static Map<Long, ColumnMappingRule> validateLogicalColumnRules( SchemaMapping mapping, Map<Long, LogicalColumn> logicalColumns ) {
        Map<Long, ColumnMappingRule> mappedColumns = new HashMap<>();
        Set<Integer> mappedPositions = new HashSet<>();
        for ( var rule : mapping.rules() ) {
            if ( rule instanceof ColumnMappingRule columnRule ) {
                long columnId = columnRule.targetLogicalColumnId();
                LogicalColumn logicalColumn = logicalColumns.get( columnId );
                if ( logicalColumn == null ) {
                    throw MappingError.Missing.logicalColumn( mapping.id(), columnId );
                }
                mappedColumns.put( columnId, columnRule );
                if ( !mappedPositions.add( logicalColumn.position ) ) {
                    throw MappingError.Duplicate.logicalPosition( mapping.id(), logicalColumn.position );
                }
            }
        }
        if ( mappedColumns.isEmpty() ) {
            throw MappingError.Validation.includedColumnRequired( mapping.id() );
        }
        return mappedColumns;
    }


    private static void validateLogicalColumnCoverage( SchemaMapping mapping, LogicalTable logicalTable, Map<Long, LogicalColumn> logicalColumns, Set<Long> mappedColumns ) {
        for ( LogicalColumn logicalColumn : logicalColumns.values() ) {
            if ( !mappedColumns.contains( logicalColumn.id ) && (logicalTable.entityType != EntityType.SOURCE || logicalTable.modifiable) ) {
                throw MappingError.Missing.columnRule( mapping.id(), logicalColumn.id );
            }
        }
    }


    private static Map<Long, Set<Long>> validatePhysicalAllocations( SchemaMapping mapping, LogicalTable logicalTable, Map<Long, LogicalColumn> logicalColumns,
            Map<Long, ColumnMappingRule> columnRules, List<AllocationEntity> allocations, AllocSnapshot alloc, Catalog catalog ) {
        Map<Long, Set<Long>> physicalColumnsByPartition = new HashMap<>();
        for ( AllocationEntity allocation : allocations ) {
            AdapterCatalog adapter = validateAdapterAllocation( mapping, allocation, catalog );
            PhysicalTable table = validatePhysicalTable( mapping, logicalTable, allocation, adapter );
            Map<Long, PhysicalColumn> tableColumns = validatePhysicalColumnUniqueness( mapping, allocation, table );
            Set<Long> allocatedColumns = validateAllocationColumns( mapping, logicalColumns, columnRules, allocation, alloc, adapter, tableColumns );
            if ( !allocatedColumns.isEmpty() ) {
                physicalColumnsByPartition.computeIfAbsent( allocation.partitionId, ignored -> new HashSet<>() ).addAll( allocatedColumns );
            }
        }
        return physicalColumnsByPartition;
    }


    private static AdapterCatalog validateAdapterAllocation( SchemaMapping mapping, AllocationEntity allocation, Catalog catalog ) {
        AdapterCatalog adapter = catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow( () ->
                MappingError.Missing.adapterCatalog( mapping.id(), allocation.id ) );
        if ( !(adapter instanceof RelAdapterCatalog) ) {
            throw MappingError.Unsupported.relationalAdapterRequired( mapping.id(), allocation.id );
        }
        AllocationEntity registeredAllocation = adapter.getAlloc( allocation.id );
        if ( registeredAllocation == null ) {
            throw MappingError.Missing.adapterAllocation( mapping.id(), allocation.id );
        }
        if ( registeredAllocation.logicalId != mapping.targetEntityId()
                || registeredAllocation.adapterId != allocation.adapterId || registeredAllocation.partitionId != allocation.partitionId
                || registeredAllocation.placementId != allocation.placementId ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), allocation.id );
        }
        return adapter;
    }


    private static PhysicalTable validatePhysicalTable( SchemaMapping mapping, LogicalTable logicalTable, AllocationEntity allocation, AdapterCatalog adapter ) {
        List<PhysicalEntity> physicals = adapter.getPhysicalsFromAllocs( allocation.id );
        if ( physicals == null || physicals.isEmpty() || physicals.get( 0 ) == null ) {
            throw MappingError.Missing.physicalTable( mapping.id(), allocation.id );
        }
        if ( !(physicals.get( 0 ) instanceof PhysicalTable table) ) {
            throw MappingError.Unsupported.relationalSourceRequired( mapping.id(), allocation.id );
        }
        if ( table.allocationId != allocation.id || table.logicalId != mapping.targetEntityId() || table.adapterId != allocation.adapterId
                || table.namespaceId != logicalTable.namespaceId ) {
            throw MappingError.Algebra.physicalBindingMismatch( mapping.id(), allocation.id );
        }
        if ( mapping.sourceModel() != table.dataModel ) {
            throw MappingError.Incompatible.sourceModel( mapping.id(), mapping.sourceModel(), allocation.id, table.dataModel );
        }
        return table;
    }


    private static Map<Long, PhysicalColumn> validatePhysicalColumnUniqueness( SchemaMapping mapping, AllocationEntity allocation, PhysicalTable table ) {
        Map<Long, PhysicalColumn> tableColumns = new HashMap<>();
        Set<Integer> physicalPositions = new HashSet<>();
        for ( PhysicalColumn column : table.columns ) {
            if ( tableColumns.putIfAbsent( column.id, column ) != null ) {
                throw MappingError.Duplicate.physicalColumn( mapping.id(), column.id, allocation.id );
            }
            if ( !physicalPositions.add( column.position ) ) {
                throw MappingError.Duplicate.physicalPosition( mapping.id(), column.position, allocation.id );
            }
        }
        return tableColumns;
    }


    private static Set<Long> validateAllocationColumns( SchemaMapping mapping, Map<Long, LogicalColumn> logicalColumns, Map<Long, ColumnMappingRule> columnRules,
            AllocationEntity allocation, AllocSnapshot alloc, AdapterCatalog adapter, Map<Long, PhysicalColumn> tableColumns ) {
        Set<Long> allocatedColumns = new HashSet<>();
        for ( AllocationColumn allocatedColumn : alloc.getColumns( allocation.placementId ) ) {
            long columnId = allocatedColumn.columnId;
            if ( allocatedColumn.logicalTableId != mapping.targetEntityId() || allocatedColumn.adapterId != allocation.adapterId ) {
                throw MappingError.Algebra.columnPlacementMismatch( mapping.id(), columnId, allocation.id );
            }
            LogicalColumn logicalColumn = logicalColumns.get( columnId );
            if ( logicalColumn == null ) {
                throw MappingError.Missing.logicalColumn( mapping.id(), columnId );
            }
            validatePhysicalColumn( mapping, logicalColumn, columnRules.get( columnId ), allocation, adapter, tableColumns.get( columnId ) );
            allocatedColumns.add( columnId );
        }
        return allocatedColumns;
    }


    private static void validatePhysicalColumn( SchemaMapping mapping, LogicalColumn logicalColumn, ColumnMappingRule rule,
            AllocationEntity allocation, AdapterCatalog adapter, PhysicalColumn tableColumn ) {
        long columnId = logicalColumn.id;
        var registered = adapter.fields.get( Pair.of( allocation.id, columnId ) );
        if ( registered == null || tableColumn == null ) {
            throw MappingError.Missing.physicalColumn( mapping.id(), columnId, allocation.id );
        }
        if ( !(registered instanceof PhysicalColumn physicalColumn) || !physicalColumn.equals( tableColumn )
                || physicalColumn.allocId != allocation.id || physicalColumn.logicalEntityId != mapping.targetEntityId()
                || physicalColumn.adapterId != allocation.adapterId ) {
            throw MappingError.Algebra.physicalColumnMismatch( mapping.id(), columnId, allocation.id );
        }
        if ( rule == null && mapping.sourceAllocationId() != null ) {
            // An excluded field on a bound allocation still has a physical column, but produces no logical value.
            return;
        }
        boolean compatible = rule instanceof RelConvertColumnMappingRule
                ? RelConvertColumnMappingRule.supports( physicalColumn.getAlgDataType( AlgDataTypeFactory.DEFAULT ), logicalColumn.getAlgDataType( AlgDataTypeFactory.DEFAULT ) )
                : hasMatchingValueType( logicalColumn, physicalColumn );
        if ( !compatible ) {
            throw MappingError.Incompatible.physicalColumn( mapping.id(), columnId, allocation.id );
        }
    }


    private static void validatePartitionCoverage( SchemaMapping mapping, List<AllocationEntity> allocations, Map<Long, Set<Long>> physicalColumnsByPartition ) {
        for ( AllocationEntity allocation : allocations ) {
            Set<Long> availableColumns = physicalColumnsByPartition.getOrDefault( allocation.partitionId, Set.of() );
            for ( var rule : mapping.rules() ) {
                if ( rule instanceof ColumnMappingRule columnRule && !availableColumns.contains( columnRule.targetLogicalColumnId() ) ) {
                    throw MappingError.Missing.partitionColumn( mapping.id(), columnRule.targetLogicalColumnId(), allocation.partitionId );
                }
            }
        }
    }


    public static boolean hasMatchingValueType( LogicalColumn logical, PhysicalColumn physical ) {
        return logical.type == physical.type
                && logical.collectionsType == physical.collectionsType
                && Objects.equals( logical.length, physical.length )
                && Objects.equals( logical.scale, physical.scale )
                && Objects.equals( logical.dimension, physical.dimension )
                && Objects.equals( logical.cardinality, physical.cardinality )
                && logical.elementsNullable == physical.elementsNullable
                && logical.collation == physical.collation;
    }
}

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

package org.polypheny.db.catalog.mapping;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.MappingUpdate.ColumnName;
import org.polypheny.db.ddl.MappingUpdate;


/**
 * MappingState resolves accepted mapping metadata for the editor.
 * It gathers the current catalog information needed to view or edit one mapping:
 * definition, adapter, logical target, columns, physical sources, revision, and editing/removal restrictions.
 */
public record MappingState(
        LogicalAdapter adapter,
        SchemaMapping mapping,
        LogicalNamespace namespace,
        LogicalTable table,
        List<LogicalColumn> columns,
        List<PhysicalTable> physicalTables,
        String revision,
        String readOnlyReason,
        String removalBlockedReason,
        Long fixedTargetEntityId,
        List<LogicalColumn> targetColumns ) {

    public MappingState( LogicalAdapter adapter, SchemaMapping mapping, LogicalNamespace namespace, LogicalTable table,
            List<LogicalColumn> columns, List<PhysicalTable> physicalTables, String revision, String readOnlyReason, String removalBlockedReason ) {
        this( adapter, mapping, namespace, table, columns, physicalTables, revision, readOnlyReason, removalBlockedReason, null, List.of() );
    }

    public static final int MAPPING_NAME_MAX_LENGTH = 255;
    public static final int IDENTIFIER_MAX_LENGTH = 100;
    public static final String IDENTIFIER_PATTERN = "[a-zA-Z_][a-zA-Z0-9_]*";


    /**
     * Returns the validation message describing mapping-name requirements.
     */
    public static String mappingNameMessage() {
        return "Enter a mapping name of 1–" + MAPPING_NAME_MAX_LENGTH + " characters.";
    }


    /**
     * Returns the validation message describing table- and column-name requirements.
     */
    public static String identifierMessage() {
        return "Use an identifier of up to " + IDENTIFIER_MAX_LENGTH + " letters, digits or underscores, starting with a letter or underscore.";
    }

    /**
     * Returns whether the mapping can be edited.
     */
    public boolean editable() {
        return readOnlyReason == null;
    }


    /**
     * Returns whether the mapping can be removed.
     */
    public boolean removable() {
        return removalBlockedReason == null;
    }

    /**
     * Checks that the revision matches and removal is permitted.
     */
    public void validateRemoval( String expectedRevision ) {
        validateRevision( expectedRevision );
        if ( !removable() ) {
            throw new EditException( "DEPENDENCY_CONFLICT", removalBlockedReason, Map.of(), List.of( table.name ) );
        }
    }


    /**
     * Checks whether a mapping rule includes the specified target column.
     */
    public boolean isIncluded( long columnId ) {
        return mapping.rules().stream().anyMatch( rule -> rule instanceof ColumnMappingRule columnRule && columnRule.targetLogicalColumnId() == columnId );
    }

    /**
     * Loads an existing mapping and resolves its state for the specified adapter.
     */
    public static MappingState load( Catalog catalog, long adapterId, long mappingId ) {
        SchemaMapping mapping = catalog.getSnapshot().mappings().getMapping( mappingId ).orElseThrow( MappingState::notFound );
        return resolve( catalog, adapterId, mapping );
    }

    /**
     * Finds the mapping’s source adapter and resolves its state.
     */
    public static MappingState load( Catalog catalog, long mappingId ) {
        var snapshot = catalog.getSnapshot();
        var mapping = snapshot.mappings().getMapping( mappingId ).orElseThrow( MappingState::notFound );
        long adapterId = SchemaMappingManager.sourceAllocations( catalog, mapping ).stream().min( Comparator.comparingLong( a -> a.id ) ).orElseThrow( MappingState::notFound ).adapterId;
        return resolve( catalog, adapterId, mapping );
    }

    /**
     * Prepares a new-target mapping draft from the selected source.
     */
    public static MappingState draft( Catalog catalog, long adapterId, long targetEntityId ) {
        return draft( catalog, adapterId, targetEntityId, null, null );
    }


    /**
     * Prepares a draft using a selected source allocation and optionally an existing target.
     */
    public static MappingState draft( Catalog catalog, long adapterId, long targetEntityId, Long existingTargetEntityId, Long sourceAllocationId ) {
        var snapshot = catalog.getSnapshot();
        var table = snapshot.rel().getTable( targetEntityId ).orElseThrow( MappingState::notFound );
        var rules = catalog.getLogicalRel( table.namespaceId ).getColumns().values().stream().filter( c -> c.tableId == table.id )
                .sorted( Comparator.comparingInt( c -> c.position ) ).<MappingRule>map( column -> new DirectColumnMappingRule( column.id ) ).toList();
        var mapping = new SchemaMapping( -1, "New mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, rules, null );
        long allocationId = snapshot.alloc().getFromLogical( table.id ).stream().filter( allocation -> allocation.adapterId == adapterId
                && (sourceAllocationId == null || sourceAllocationId == allocation.id) ).min( Comparator.comparingLong( allocation -> allocation.id ) ).orElseThrow( MappingState::notFound ).id;
        mapping = mapping.withSourceAllocation( allocationId );
        MappingState source = resolve( catalog, adapterId, mapping );
        if ( source.physicalTables().size() != 1 ) {
            throw new EditException( "UNSUPPORTED_MAPPING", "Creating mappings currently requires a single physical source table." );
        }
        String sourceName = source.physicalTables().get( 0 ).name;
        String baseName = source.adapter().uniqueName + "_" + sourceName + "_mapping";
        String name = baseName;
        for ( int suffix = 1; SchemaMappingManager.isMappingNameTaken( snapshot.mappings().getMappings(), name, -1 ); suffix++ ) {
            name = baseName + "_" + suffix;
        }
        String targetBase = sourceName.replaceAll( "[^a-zA-Z0-9_]", "_" );
        if ( targetBase.isEmpty() || Character.isDigit( targetBase.charAt( 0 ) ) ) {
            targetBase = "source_" + targetBase;
        }
        targetBase = targetBase.substring( 0, Math.min( targetBase.length(), IDENTIFIER_MAX_LENGTH - 16 ) ) + "_mapped";
        String targetName = targetBase;
        for ( int suffix = 2; snapshot.rel().getTable( table.namespaceId, targetName ).isPresent(); suffix++ ) {
            targetName = targetBase + "_" + suffix;
        }
        List<LogicalColumn> draftColumns = source.physicalTables().get( 0 ).columns.stream().map( column -> new LogicalColumn(
                column.id, column.name, table.id, table.namespaceId, column.position + 1, column.type, column.collectionsType,
                column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.elementsNullable, column.collation, column.defaultValue ) ).toList();
        rules = draftColumns.stream().<MappingRule>map( column -> new DirectColumnMappingRule( column.id ) ).toList();
        mapping = new SchemaMapping( -1, mapping.name(), mapping.kind(), null, table.id, mapping.sourceModel(), mapping.targetModel(), rules, allocationId );
        if ( existingTargetEntityId != null ) {
            var target = snapshot.rel().getTable( existingTargetEntityId ).orElseThrow( MappingState::notFound );
            validateExistingTarget( catalog, target );
            if ( SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, source.mapping(), target.id ) ) {
                throw new EditException( "VALIDATION_FAILED", "This source already has a mapping to the selected target.",
                        Map.of( "sourceAllocationId", "Choose a source that is not already mapped to this target." ), List.of() );
            }
            var targetNamespace = snapshot.getNamespace( target.namespaceId ).orElseThrow();
            var targetColumns = snapshot.rel().getColumns( target.id );
            var proposed = draftColumns.stream().map( column -> {
                var match = targetColumns.stream().filter( targetColumn -> targetNamespace.caseSensitive
                        ? targetColumn.name.equals( column.name ) : targetColumn.name.equalsIgnoreCase( column.name ) ).findFirst();
                return match.<LogicalColumn>map( targetColumn -> column.toBuilder().name( targetColumn.name ).build() ).orElse( column );
            } ).toList();
            return new MappingState( source.adapter(), new SchemaMapping( -1, name, mapping.kind(), null, target.id,
                    mapping.sourceModel(), mapping.targetModel(), rules, allocationId ), targetNamespace, target, proposed,
                    source.physicalTables(), source.revision() + ":" + targetRevision( catalog, target ), source.readOnlyReason(), source.removalBlockedReason(), target.id, targetColumns );
        }
        return new MappingState( source.adapter(), SchemaMappingManager.renameMapping( mapping, name ), source.namespace(),
                source.table().toBuilder().name( targetName ).build(), draftColumns, source.physicalTables(), source.revision(), source.readOnlyReason(), source.removalBlockedReason() );
    }

    /**
     * Collects the mapping catalog metadata and calculates its revision and editing/removal restrictions.
     * Revision is a fingerprint of the mapping current catalog state,
     * used to detect changes made after the editor was opened.
     */
    private static MappingState resolve( Catalog catalog, long adapterId, SchemaMapping mapping ) {
        Snapshot snapshot = catalog.getSnapshot();
        LogicalAdapter adapter = snapshot.getAdapter( adapterId ).orElseThrow( MappingState::notFound );
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL ) {
            throw notFound();
        }
        LogicalTable table = snapshot.rel().getTable( mapping.targetEntityId() ).orElseThrow( MappingState::notFound );
        List<AllocationEntity> allocations = SchemaMappingManager.sourceAllocations( catalog, mapping ).stream()
                .sorted( Comparator.comparingLong( a -> a.id ) ).toList();
        if ( allocations.stream().noneMatch( a -> a.adapterId == adapterId ) ) {
            throw notFound();
        }
        LogicalNamespace namespace = snapshot.getNamespace( table.namespaceId ).orElseThrow( MappingState::notFound );
        // Keep accepted source metadata available to the editor even when a field is excluded from the published schema.
        List<LogicalColumn> columns = catalog.getLogicalRel( namespace.id ).getColumns().values().stream().filter( c -> c.tableId == table.id )
                .sorted( Comparator.comparingInt( c -> c.position ) ).toList();
        List<PhysicalTable> physicalTables = new ArrayList<>();
        for ( AllocationEntity allocation : allocations ) {
            var physical = catalog.getAdapterCatalog( allocation.adapterId ).orElseThrow( MappingState::notFound )
                    .getPhysicalsFromAllocs( allocation.id );
            if ( physical == null || physical.isEmpty() || physical.stream().anyMatch( p -> !(p instanceof PhysicalTable) ) ) {
                throw new EditException( "SOURCE_UNAVAILABLE", "Accepted physical source metadata is unavailable." );
            }
            physical.stream().map( PhysicalTable.class::cast ).sorted( Comparator.comparingLong( p -> p.id ) ).forEach( physicalTables::add );
        }
        String reason = null;
        if ( adapter.type != AdapterType.SOURCE || table.entityType != EntityType.SOURCE || table.modifiable ) {
            reason = "Only read-only imported source tables can be edited here.";
        } else if ( mapping.rules().stream().anyMatch( rule -> !(rule instanceof DirectColumnMappingRule) ) ) {
            reason = "This mapping uses rules that this editor does not support.";
        } else if ( allocations.stream().anyMatch( a -> a.adapterId != adapterId )
                || !adapter.adapterName.equalsIgnoreCase( "CSV" )
                || !(catalog.getAdapterCatalog( adapterId ).orElse( null ) instanceof RelAdapterCatalog) ) {
            reason = "Editing is currently supported for CSV source mappings on one adapter.";
        }
        String removalReason = SchemaMappingManager.removalBlockedReason( catalog, snapshot.mappings().getMappings(), mapping, table );
        if ( removalReason == null && snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() == 1 && (!snapshot.rel().getConnectedViews( table.id ).isEmpty() || !snapshot.rel().getExportedKeys( table.id ).isEmpty()) ) {
            removalReason = "Dependent views or foreign keys prevent dropping this mapping's logical target.";
        }
        return new MappingState( adapter, mapping, namespace, table, columns, List.copyOf( physicalTables ),
                revision( adapter, mapping, namespace, table, columns, allocations, physicalTables ), reason,
                reason != null ? reason : removalReason, snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() > 1 ? table.id : null,
                snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() > 1 ? snapshot.rel().getColumns( table.id ) : List.of() );
    }

    /**
     * Validates the proposed changes, target compatibility, and dependencies, then returns a normalized update command.
     */
    public MappingUpdate validate( MappingUpdate update, Snapshot snapshot ) {
        if ( update.adapterId() != adapter.id || update.mappingId() != mapping.id() ) {
            throw notFound();
        }
        validateRevision( update.expectedRevision() );
        LogicalNamespace targetNamespace = update.targetNamespaceId() == null ? namespace
                : snapshot.getNamespace( update.targetNamespaceId() ).orElseThrow( () -> new EditException( "VALIDATION_FAILED",
                        "Choose an existing target namespace.", Map.of( "targetNamespaceId", "This namespace is no longer available." ), List.of() ) );
        if ( targetNamespace.dataModel != DataModel.RELATIONAL || targetNamespace.hidden ) {
            throw new EditException( "VALIDATION_FAILED", "Choose a relational target namespace.",
                    Map.of( "targetNamespaceId", "Choose a relational target namespace." ), List.of() );
        }
        Map<String, String> errors = new LinkedHashMap<>();
        String label = update.mappingName() == null ? "" : update.mappingName().trim();
        if ( label.isEmpty() || label.length() > MAPPING_NAME_MAX_LENGTH ) {
            errors.put( "mappingName", mappingNameMessage() );
        } else if ( SchemaMappingManager.isMappingNameTaken( snapshot.mappings().getMappings(), label, mapping.id() ) ) {
            errors.put( "mappingName", "A mapping with this name already exists." );
        }
        String tableName = validateName( update.logicalTableName(), table.name, "logicalTableName", errors, targetNamespace );
        if ( fixedTargetEntityId == null && snapshot.rel().getTables( targetNamespace.id, null ).stream()
                .anyMatch( t -> (mapping.id() < 0 || t.id != table.id) && key( t.name, targetNamespace ).equals( key( tableName, targetNamespace ) ) ) ) {
            errors.put( "logicalTableName", "An entity with this name already exists in the namespace." );
        }
        Map<Long, ColumnName> requested = new HashMap<>();
        if ( update.columns() == null ) {
            errors.put( "columns", "Supply the complete existing column set." );
        } else {
            for ( ColumnName column : update.columns() ) {
                if ( column == null || requested.containsKey( column.logicalColumnId() ) ) {
                    errors.put( "columns", "Column IDs must be present and unique." );
                } else {
                    requested.put( column.logicalColumnId(), column );
                }
            }
        }
        if ( !requested.keySet().equals( columns.stream().map( c -> c.id ).collect( java.util.stream.Collectors.toSet() ) ) ) {
            errors.put( "columns", "The request must contain exactly the existing logical column IDs." );
        }
        List<ColumnName> names = new ArrayList<>();
        boolean namespaceChanged = targetNamespace.id != namespace.id;
        boolean logicalChanged = namespaceChanged || !tableName.equals( table.name );
        for ( LogicalColumn column : columns ) {
            String field = "columns." + column.id;
            ColumnName requestedColumn = requested.get( column.id );
            boolean included = requestedColumn == null || requestedColumn.included() == null ? isIncluded( column.id ) : requestedColumn.included();
            String name = included && requestedColumn != null ? validateName( requestedColumn.logicalName(), column.name, field, errors, targetNamespace ) : column.name;
            if ( fixedTargetEntityId == null && mapping.id() >= 0 && columns.stream().anyMatch( other -> other.id != column.id && key( other.name, targetNamespace ).equals( key( name, targetNamespace ) ) ) ) {
                errors.put( field, "This name belongs to another column. Simultaneous name swaps are not supported." );
            }
            if ( (fixedTargetEntityId == null || included) && names.stream().filter( other -> fixedTargetEntityId == null || other.included() ).anyMatch( other -> key( other.logicalName(), targetNamespace ).equals( key( name, targetNamespace ) ) ) ) {
                errors.put( field, "Logical column names must be unique." );
            }
            if ( included && physicalTables.stream().flatMap( physical -> physical.columns.stream() ).noneMatch( physical -> physical.id == column.id ) ) {
                errors.put( field, "The selected source does not contain this field." );
            }
            logicalChanged |= !name.equals( column.name ) || included != isIncluded( column.id );
            names.add( new ColumnName( column.id, name, included ) );
        }
        if ( names.stream().noneMatch( ColumnName::included ) ) {
            errors.put( "columns", "Include at least one field in the target entity." );
        }
        if ( fixedTargetEntityId != null ) {
            if ( targetNamespace.id != namespace.id || !tableName.equals( table.name ) ) {
                errors.put( "logicalTableName", "This mapping uses an existing target. Its table and namespace are fixed." );
            }
            if ( mapping.id() >= 0 && logicalChanged ) {
                errors.put( "columns", "Mappings sharing a target must preserve its existing schema." );
            }
            if ( mapping.id() < 0 ) {
                var targetColumns = snapshot.rel().getColumns( table.id );
                var included = names.stream().filter( ColumnName::included ).toList();
                if ( included.size() != targetColumns.size() || targetColumns.stream().anyMatch( targetColumn -> included.stream().noneMatch( requestedColumn ->
                        key( requestedColumn.logicalName(), namespace ).equals( key( targetColumn.name, namespace ) )
                                && compatible( columns.stream().filter( sourceColumn -> sourceColumn.id == requestedColumn.logicalColumnId() ).findFirst().orElseThrow(), targetColumn ) ) ) ) {
                    errors.put( "columns", "The included source fields must match all existing target columns in name, type and nullability." );
                }
            }
        }
        if ( !errors.isEmpty() ) {
            throw new EditException( "VALIDATION_FAILED", "Correct the highlighted mapping fields.", errors, List.of() );
        }
        if ( logicalChanged && mapping.id() >= 0 ) {
            List<String> dependencies = new ArrayList<>( snapshot.rel().getConnectedViews( table.id ).stream().map( v -> v.name ).toList() );
            Set<Long> excluded = names.stream().filter( c -> !c.included() ).map( ColumnName::logicalColumnId ).collect( Collectors.toSet() );
            snapshot.rel().getExportedKeys( table.id ).stream().filter( key -> key.referencedKeyFieldIds.stream().anyMatch( excluded::contains ) )
                    .map( key -> key.name ).forEach( dependencies::add );
            if ( namespaceChanged ) {
                snapshot.rel().getExportedKeys( table.id ).stream().map( key -> key.name ).forEach( dependencies::add );
                snapshot.rel().getForeignKeys( table.id ).stream().map( key -> key.name ).forEach( dependencies::add );
            }
            if ( !dependencies.isEmpty() ) {
                throw new EditException( "DEPENDENCY_CONFLICT", "Dependent views or foreign keys prevent changing this logical schema.", Map.of(), dependencies );
            }
        }
        return new MappingUpdate( adapter.id, mapping.id(), revision, label, tableName, List.copyOf( names ), targetNamespace.id, update.existingTargetEntityId(), update.sourceAllocationId() );
    }

    /**
     * Rejects editing when unsupported or when the submitted revision differs from the current state.
     */
    public void validateRevision( String expectedRevision ) {
        if ( !editable() ) {
            throw new EditException( "UNSUPPORTED_MAPPING", readOnlyReason );
        }
        if ( !Objects.equals( revision, expectedRevision ) ) {
            throw new EditException( "REVISION_CONFLICT", "This mapping changed after it was opened. Reload the saved version before editing it again." );
        }
    }


    /**
     * Validates and normalizes a table or column name while allowing unchanged imported identifiers.
     */
    private String validateName( String value, String original, String field, Map<String, String> errors, LogicalNamespace targetNamespace ) {
        String name = value == null ? "" : value.trim();
        // Existing identifiers from imported sources can predate the editor's naming convention.
        if ( name.equals( original ) ) {
            return original;
        }
        if ( name.length() > IDENTIFIER_MAX_LENGTH || !name.matches( IDENTIFIER_PATTERN ) ) {
            errors.put( field, identifierMessage() );
        }
        return key( name, targetNamespace );
    }


    /**
     * Normalizes an identifier according to the namespace’s case-sensitivity setting.
     */
    private String key( String name, LogicalNamespace targetNamespace ) {
        return targetNamespace.caseSensitive ? name : name.toLowerCase( Locale.ROOT );
    }


    /**
     * Calculates a fingerprint of the mapping and its associated catalog metadata to detect changes.
     */
    private static String revision( LogicalAdapter adapter, SchemaMapping mapping, LogicalNamespace namespace, LogicalTable table,
            List<LogicalColumn> columns, List<AllocationEntity> allocations, List<PhysicalTable> physicalTables ) {
        try {
            MessageDigest digest = MessageDigest.getInstance( "SHA-256" );
            hash( digest, adapter.id, adapter.adapterName, adapter.type, mapping.id(), mapping.name(), mapping.kind(),
                    mapping.sourceEntityId(), mapping.targetEntityId(), mapping.sourceModel(), mapping.targetModel(), mapping.sourceAllocationId(),
                    namespace.id, namespace.name, namespace.caseSensitive, table.id, table.name, table.entityType, table.modifiable );
            adapter.settings.entrySet().stream().sorted( Map.Entry.comparingByKey() ).forEach( e -> hash( digest, e.getKey(), e.getValue() ) );
            mapping.rules().forEach( r -> hash( digest, r.getClass().getName(), r.toString() ) );
            columns.forEach( c -> hash( digest, c.id, c.name, c.position, c.type, c.collectionsType, c.length, c.scale, c.dimension, c.cardinality, c.nullable ) );
            allocations.forEach( a -> hash( digest, a.id, a.adapterId, a.placementId, a.partitionId, a.logicalId ) );
            physicalTables.forEach( p -> {
                hash( digest, p.id, p.adapterId, p.allocationId, p.namespaceName, p.name );
                p.columns.stream().sorted( Comparator.comparingInt( c -> c.position ) ).forEach( c ->
                        hash( digest, c.id, c.name, c.logicalName, c.position, c.type, c.collectionsType, c.length, c.scale, c.dimension, c.cardinality, c.nullable ) );
            } );
            return HexFormat.of().formatHex( digest.digest() );
        } catch ( NoSuchAlgorithmException e ) {
            throw new IllegalStateException( "SHA-256 is unavailable", e );
        }
    }

    /**
     * Checks whether two columns match in type, size, collection properties, nullability, and collation.
     */
    public static boolean compatible( LogicalColumn source, LogicalColumn target ) {
        return source.type == target.type && source.collectionsType == target.collectionsType
                && Objects.equals( source.length, target.length ) && Objects.equals( source.scale, target.scale )
                && Objects.equals( source.dimension, target.dimension ) && Objects.equals( source.cardinality, target.cardinality )
                && source.nullable == target.nullable && source.elementsNullable == target.elementsNullable && source.collation == target.collation;
    }

    /**
     * Checks that an existing target is a read-only source table with a single partition.
     */
    private static void validateExistingTarget( Catalog catalog, LogicalTable target ) {
        if ( target.entityType != EntityType.SOURCE || target.modifiable ) {
            throw new EditException( "UNSUPPORTED_MAPPING", "Existing-target mappings currently require an imported or mapping-backed table." );
        }
        if ( catalog.getSnapshot().alloc().getFromLogical( target.id ).stream().map( allocation -> allocation.partitionId ).distinct().count() != 1 ) {
            throw new EditException( "UNSUPPORTED_MAPPING", "Existing-target mappings require a single target partition." );
        }
    }

    /**
     * Calculates a fingerprint of an existing target’s metadata and mappings
     * to detect changes during mapping creation.
     */
    private static String targetRevision( Catalog catalog, LogicalTable target ) {
        try {
            var digest = MessageDigest.getInstance( "SHA-256" );
            hash( digest, target.id, target.name, target.namespaceId, target.entityType, target.modifiable );
            catalog.getSnapshot().rel().getColumns( target.id ).forEach( column -> hash( digest, column.id, column.name, column.position,
                    column.type, column.collectionsType, column.length, column.scale, column.nullable, column.elementsNullable ) );
            catalog.getSnapshot().mappings().getMappingsForLogicalEntity( target.id ).forEach( mapping -> hash( digest, mapping ) );
            return HexFormat.of().formatHex( digest.digest() );
        } catch ( NoSuchAlgorithmException e ) {
            throw new IllegalStateException( e );
        }
    }

    /**
     * Adds values to a revision hash with length prefixes that preserve their boundaries.
     */
    private static void hash( MessageDigest digest, Object... values ) {
        for ( Object value : values ) {
            byte[] bytes = String.valueOf( value ).getBytes( StandardCharsets.UTF_8 );
            digest.update( ByteBuffer.allocate( Integer.BYTES ).putInt( bytes.length ).array() );
            digest.update( bytes );
        }
    }


    /**
     * Creates the standard exception indicating that the source adapter or mapping is unavailable.
     */
    public static EditException notFound() {
        return new EditException( "NOT_FOUND", "The source adapter or its mapping is no longer available." );
    }

    /**
     * EditException class
     */
    public static class EditException extends RuntimeException {

        public final String code;
        public final Map<String, String> fieldErrors;
        public final List<String> blockingEntities;

        /**
         * Creates a mapping-operation error without field errors or blocking-entity details.
         */
        public EditException( String code, String message ) {
            this( code, message, Map.of(), List.of() );
        }

        /**
         * Creates a mapping-operation error containing its code, message, field errors, and blocking entities.
         */
        public EditException( String code, String message, Map<String, String> fieldErrors, List<String> blockingEntities ) {
            super( message );
            this.code = code;
            this.fieldErrors = Map.copyOf( fieldErrors );
            this.blockingEntities = List.copyOf( blockingEntities );
        }

    }

}

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
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.MappingUpdate;
import org.polypheny.db.ddl.MappingUpdate.ColumnName;


/**
 * MappingState resolves accepted mapping metadata for the editor.
 * It gathers the current catalog information needed to view or edit one mapping.
 */
public record MappingState(
        LogicalAdapter adapter,
        SchemaMapping mapping,
        LogicalNamespace namespace,
        LogicalTable table,
        List<LogicalColumn> columns,
        List<PhysicalTable> physicalTables,
        String revision,
        String readOnlyReason ) {

    public static final int MAPPING_NAME_MAX_LENGTH = 255;
    public static final int IDENTIFIER_MAX_LENGTH = 100;
    public static final String IDENTIFIER_PATTERN = "[a-zA-Z_][a-zA-Z0-9_]*";


    public static String mappingNameMessage() {
        return "Enter a mapping name of 1–" + MAPPING_NAME_MAX_LENGTH + " characters.";
    }


    public static String identifierMessage() {
        return "Use an identifier of up to " + IDENTIFIER_MAX_LENGTH + " letters, digits or underscores, starting with a letter or underscore.";
    }


    public boolean editable() {
        return readOnlyReason == null;
    }


    public boolean isIncluded( long columnId ) {
        return mapping.rules().stream().anyMatch( rule -> rule instanceof ColumnMappingRule columnRule && columnRule.targetLogicalColumnId() == columnId );
    }


    public static MappingState load( Catalog catalog, long adapterId, long mappingId ) {
        Snapshot snapshot = catalog.getSnapshot();
        LogicalAdapter adapter = snapshot.getAdapter( adapterId ).orElseThrow( MappingState::notFound );
        SchemaMapping mapping = snapshot.mappings().getMapping( mappingId ).orElseThrow( MappingState::notFound );
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL ) {
            throw notFound();
        }
        LogicalTable table = snapshot.rel().getTable( mapping.targetEntityId() ).orElseThrow( MappingState::notFound );
        List<AllocationEntity> allocations = snapshot.alloc().getFromLogical( table.id ).stream()
                .sorted( Comparator.comparingLong( a -> a.id ) ).toList();
        if ( adapter.type != AdapterType.SOURCE || allocations.stream().noneMatch( a -> a.adapterId == adapterId ) ) {
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
        if ( table.entityType != EntityType.SOURCE || table.modifiable ) {
            reason = "Only read-only imported source tables can be edited here.";
        } else if ( mapping.rules().stream().anyMatch( rule -> !(rule instanceof DirectColumnMappingRule) ) ) {
            reason = "This mapping uses rules that this editor does not support.";
        } else if ( allocations.stream().anyMatch( a -> a.adapterId != adapterId )
                || !adapter.adapterName.equalsIgnoreCase( "CSV" )
                || !(catalog.getAdapterCatalog( adapterId ).orElse( null ) instanceof RelAdapterCatalog) ) {
            reason = "Editing is currently supported for CSV source mappings on one adapter.";
        }
        return new MappingState( adapter, mapping, namespace, table, columns, List.copyOf( physicalTables ),
                revision( adapter, mapping, namespace, table, columns, allocations, physicalTables ), reason );
    }


    public MappingUpdate validate( MappingUpdate update, Snapshot snapshot ) {
        if ( update.adapterId() != adapter.id || update.mappingId() != mapping.id() ) {
            throw notFound();
        }
        if ( !editable() ) {
            throw new EditException( "UNSUPPORTED_MAPPING", readOnlyReason );
        }
        if ( !Objects.equals( revision, update.expectedRevision() ) ) {
            throw new EditException( "REVISION_CONFLICT", "This mapping changed after it was opened. Reload the saved version before editing it again." );
        }
        Map<String, String> errors = new LinkedHashMap<>();
        String label = update.mappingName() == null ? "" : update.mappingName().trim();
        if ( label.isEmpty() || label.length() > MAPPING_NAME_MAX_LENGTH ) {
            errors.put( "mappingName", mappingNameMessage() );
        }
        String tableName = validateName( update.logicalTableName(), table.name, "logicalTableName", errors );
        if ( snapshot.rel().getTables( namespace.id, null ).stream()
                .anyMatch( t -> t.id != table.id && key( t.name ).equals( key( tableName ) ) ) ) {
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
        boolean logicalChanged = !tableName.equals( table.name );
        for ( LogicalColumn column : columns ) {
            String field = "columns." + column.id;
            ColumnName requestedColumn = requested.get( column.id );
            boolean included = requestedColumn == null || requestedColumn.included() == null ? isIncluded( column.id ) : requestedColumn.included();
            String name = included && requestedColumn != null ? validateName( requestedColumn.logicalName(), column.name, field, errors ) : column.name;
            if ( columns.stream().anyMatch( other -> other.id != column.id && key( other.name ).equals( key( name ) ) ) ) {
                errors.put( field, "This name belongs to another column. Simultaneous name swaps are not supported." );
            }
            if ( names.stream().anyMatch( other -> key( other.logicalName() ).equals( key( name ) ) ) ) {
                errors.put( field, "Logical column names must be unique." );
            }
            logicalChanged |= !name.equals( column.name ) || included != isIncluded( column.id );
            names.add( new ColumnName( column.id, name, included ) );
        }
        if ( names.stream().noneMatch( ColumnName::included ) ) {
            errors.put( "columns", "Include at least one field in the target entity." );
        }
        if ( !errors.isEmpty() ) {
            throw new EditException( "VALIDATION_FAILED", "Correct the highlighted mapping fields.", errors, List.of() );
        }
        if ( logicalChanged ) {
            List<String> dependencies = new ArrayList<>( snapshot.rel().getConnectedViews( table.id ).stream().map( v -> v.name ).toList() );
            Set<Long> excluded = names.stream().filter( c -> !c.included() ).map( ColumnName::logicalColumnId ).collect( Collectors.toSet() );
            snapshot.rel().getExportedKeys( table.id ).stream().filter( key -> key.referencedKeyFieldIds.stream().anyMatch( excluded::contains ) )
                    .map( key -> key.name ).forEach( dependencies::add );
            if ( !dependencies.isEmpty() ) {
                throw new EditException( "DEPENDENCY_CONFLICT", "Dependent views or foreign keys prevent changing this logical schema.", Map.of(), dependencies );
            }
        }
        return new MappingUpdate( adapter.id, mapping.id(), revision, label, tableName, List.copyOf( names ) );
    }


    private String validateName( String value, String original, String field, Map<String, String> errors ) {
        String name = value == null ? "" : value.trim();
        // Existing identifiers from imported sources can predate the editor's naming convention.
        if ( name.equals( original ) ) {
            return original;
        }
        if ( name.length() > IDENTIFIER_MAX_LENGTH || !name.matches( IDENTIFIER_PATTERN ) ) {
            errors.put( field, identifierMessage() );
        }
        return key( name );
    }


    private String key( String name ) {
        return namespace.caseSensitive ? name : name.toLowerCase( Locale.ROOT );
    }


    private static String revision( LogicalAdapter adapter, SchemaMapping mapping, LogicalNamespace namespace, LogicalTable table,
            List<LogicalColumn> columns, List<AllocationEntity> allocations, List<PhysicalTable> physicalTables ) {
        try {
            MessageDigest digest = MessageDigest.getInstance( "SHA-256" );
            hash( digest, adapter.id, adapter.adapterName, adapter.type, mapping.id(), mapping.name(), mapping.kind(),
                    mapping.sourceEntityId(), mapping.targetEntityId(), mapping.sourceModel(), mapping.targetModel(),
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


    private static void hash( MessageDigest digest, Object... values ) {
        for ( Object value : values ) {
            byte[] bytes = String.valueOf( value ).getBytes( StandardCharsets.UTF_8 );
            digest.update( ByteBuffer.allocate( Integer.BYTES ).putInt( bytes.length ).array() );
            digest.update( bytes );
        }
    }


    public static EditException notFound() {
        return new EditException( "NOT_FOUND", "The source adapter or its mapping is no longer available." );
    }


    public static class EditException extends RuntimeException {

        public final String code;
        public final Map<String, String> fieldErrors;
        public final List<String> blockingEntities;


        public EditException( String code, String message ) {
            this( code, message, Map.of(), List.of() );
        }


        public EditException( String code, String message, Map<String, String> fieldErrors, List<String> blockingEntities ) {
            super( message );
            this.code = code;
            this.fieldErrors = Map.copyOf( fieldErrors );
            this.blockingEntities = List.copyOf( blockingEntities );
        }

    }

}

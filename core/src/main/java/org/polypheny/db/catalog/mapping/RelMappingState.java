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
import java.util.stream.Stream;
import org.polypheny.db.algebra.mapping.RuleRepository;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
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
import org.polypheny.db.catalog.entity.mapping.rules.RelConvertColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RelCopyColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.RelMappingUpdate.ColumnName;
import org.polypheny.db.ddl.RelMappingUpdate;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.ScalarConversions;


/**
 * RelMappingState resolves accepted mapping metadata for the editor.
 * It gathers the current catalog information needed to view or edit one mapping:
 * definition, adapter, logical target, columns, physical sources, revision, and editing/removal restrictions.
 */
public record RelMappingState(
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

    public RelMappingState( LogicalAdapter adapter, SchemaMapping mapping, LogicalNamespace namespace, LogicalTable table,
            List<LogicalColumn> columns, List<PhysicalTable> physicalTables, String revision, String readOnlyReason, String removalBlockedReason ) {
        this( adapter, mapping, namespace, table, columns, physicalTables, revision, readOnlyReason, removalBlockedReason, null, List.of() );
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
            throw MappingError.Conflict.removalBlocked( mapping.name(), removalBlockedReason, List.of( table.name ) );
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
    public static RelMappingState load( Catalog catalog, long adapterId, long mappingId ) {
        SchemaMapping mapping = catalog.getSnapshot().mappings().getMapping( mappingId ).orElseThrow( MappingError.Missing::resource );
        return resolve( catalog, adapterId, mapping );
    }

    /**
     * Finds the mapping’s source adapter and resolves its state.
     */
    public static RelMappingState load( Catalog catalog, long mappingId ) {
        var snapshot = catalog.getSnapshot();
        var mapping = snapshot.mappings().getMapping( mappingId ).orElseThrow( MappingError.Missing::resource );
        long adapterId = SchemaMappingManager.sourceAllocations( catalog, mapping ).stream().min( Comparator.comparingLong( a -> a.id ) )
                .orElseThrow( () -> MappingError.Missing.sourceAllocation( mapping.id() ) ).adapterId;
        return resolve( catalog, adapterId, mapping );
    }

    /**
     * Prepares a new-target mapping draft from the selected source.
     */
    public static RelMappingState draft( Catalog catalog, long adapterId, long targetEntityId ) {
        return draft( catalog, adapterId, targetEntityId, null, null );
    }


    /**
     * Prepares a draft using a selected source allocation and optionally an existing target.
     */
    public static RelMappingState draft( Catalog catalog, long adapterId, long targetEntityId, Long existingTargetEntityId, Long sourceAllocationId ) {
        var snapshot = catalog.getSnapshot();
        var table = snapshot.rel().getTable( targetEntityId ).orElseThrow( MappingError.Missing::resource );
        var rules = catalog.getLogicalRel( table.namespaceId ).getColumns().values().stream().filter( c -> c.tableId == table.id )
                .sorted( Comparator.comparingInt( c -> c.position ) ).<MappingRule>map( column -> new RelCopyColumnMappingRule( column.id ) ).toList();
        var mapping = new SchemaMapping( -1, "New mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, table.id,
                DataModel.RELATIONAL, DataModel.RELATIONAL, rules, null );
        long allocationId = snapshot.alloc().getFromLogical( table.id ).stream().filter( allocation -> allocation.adapterId == adapterId
                && (sourceAllocationId == null || sourceAllocationId == allocation.id) ).min( Comparator.comparingLong( allocation -> allocation.id ) ).orElseThrow( MappingError.Missing::resource ).id;
        mapping = mapping.withSourceAllocation( allocationId );
        RelMappingState source = resolve( catalog, adapterId, mapping );
        if ( source.physicalTables().size() != 1 ) {
            throw MappingError.Unsupported.singlePhysicalSourceRequired();
        }
        String sourceName = source.physicalTables().get( 0 ).name;
        String name = SchemaMappingManager.availableDefaultMappingName( snapshot.mappings().getMappings(), source.adapter().uniqueName, sourceName );
        String targetBase = sourceName.replaceAll( "[^a-zA-Z0-9_]", "_" );
        if ( targetBase.isEmpty() || Character.isDigit( targetBase.charAt( 0 ) ) ) {
            targetBase = "source_" + targetBase;
        }
        targetBase = targetBase.substring( 0, Math.min( targetBase.length(), SchemaMappingManager.IDENTIFIER_MAX_LENGTH - 16 ) ) + "_mapped";
        String targetName = targetBase;
        for ( int suffix = 2; snapshot.rel().getTable( table.namespaceId, targetName ).isPresent(); suffix++ ) {
            targetName = targetBase + "_" + suffix;
        }
        List<LogicalColumn> draftColumns = source.physicalTables().get( 0 ).columns.stream().map( column -> new LogicalColumn(
                column.id, column.name, table.id, table.namespaceId, column.position + 1, column.type, column.collectionsType,
                column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.elementsNullable, column.collation, column.defaultValue ) ).toList();
        rules = draftColumns.stream().<MappingRule>map( column -> new RelCopyColumnMappingRule( column.id ) ).toList();
        mapping = new SchemaMapping( -1, mapping.name(), mapping.kind(), null, table.id, mapping.sourceModel(), mapping.targetModel(), rules, allocationId );
        if ( existingTargetEntityId != null ) {
            var target = snapshot.rel().getTable( existingTargetEntityId ).orElseThrow( MappingError.Missing::resource );
            validateExistingTarget( catalog, target );
            if ( SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, source.mapping(), target.id ) ) {
                throw MappingError.Duplicate.sourceTargetPair();
            }
            var targetNamespace = snapshot.getNamespace( target.namespaceId ).orElseThrow( MappingError.Missing::resource );
            var targetColumns = snapshot.rel().getColumns( target.id );
            var proposed = draftColumns.stream().map( column -> {
                var match = targetColumns.stream().filter( targetColumn -> targetNamespace.caseSensitive
                        ? targetColumn.name.equals( column.name ) : targetColumn.name.equalsIgnoreCase( column.name ) ).findFirst();
                return match.<LogicalColumn>map( targetColumn -> column.toBuilder().name( targetColumn.name ).build() ).orElse( column );
            } ).toList();
            return new RelMappingState( source.adapter(), new SchemaMapping( -1, name, mapping.kind(), null, target.id,
                    mapping.sourceModel(), mapping.targetModel(), rules, allocationId ), targetNamespace, target, proposed,
                    source.physicalTables(), source.revision() + ":" + targetRevision( catalog, target ), source.readOnlyReason(), source.removalBlockedReason(), target.id, targetColumns );
        }
        return new RelMappingState( source.adapter(), SchemaMappingManager.renameMapping( mapping, name ), source.namespace(),
                source.table().toBuilder().name( targetName ).build(), draftColumns, source.physicalTables(), source.revision(), source.readOnlyReason(), source.removalBlockedReason() );
    }

    /**
     * Collects the mapping catalog metadata and calculates its revision and editing/removal restrictions.
     * Revision is a fingerprint of the mapping current catalog state,
     * used to detect changes made after the editor was opened.
     */
    private static RelMappingState resolve( Catalog catalog, long adapterId, SchemaMapping mapping ) {
        Snapshot snapshot = catalog.getSnapshot();
        LogicalAdapter adapter = snapshot.getAdapter( adapterId ).orElseThrow( MappingError.Missing::resource );
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL ) {
            throw MappingError.Unsupported.kind( mapping.id(), mapping.kind() );
        }
        LogicalTable table = snapshot.rel().getTable( mapping.targetEntityId() ).orElseThrow( MappingError.Missing::resource );
        List<AllocationEntity> allocations = SchemaMappingManager.sourceAllocations( catalog, mapping ).stream()
                .sorted( Comparator.comparingLong( a -> a.id ) ).toList();
        if ( allocations.stream().noneMatch( a -> a.adapterId == adapterId ) ) {
            throw MappingError.Missing.resource();
        }
        LogicalNamespace namespace = snapshot.getNamespace( table.namespaceId ).orElseThrow( MappingError.Missing::resource );
        // Keep accepted source metadata available to the editor even when a field is excluded from the published schema.
        List<LogicalColumn> columns = catalog.getLogicalRel( namespace.id ).getColumns().values().stream().filter( c -> c.tableId == table.id )
                .sorted( Comparator.comparingInt( c -> c.position ) ).toList();
        List<PhysicalTable> physicalTables = new ArrayList<>();
        for ( AllocationEntity allocation : allocations ) {
            var physical = catalog.getAdapterCatalog( allocation.adapterId )
                    .orElseThrow( () -> MappingError.Missing.adapterCatalog( allocation.adapterId ) )
                    .getPhysicalsFromAllocs( allocation.id );
            if ( physical == null || physical.isEmpty() || physical.stream().anyMatch( Objects::isNull ) ) {
                throw MappingError.Missing.physicalMetadata( allocation.id );
            }
            if ( physical.stream().anyMatch( p -> !(p instanceof PhysicalTable) ) ) {
                throw MappingError.Unsupported.relationalSourceRequired( mapping.id(), allocation.id );
            }
            physical.stream().map( PhysicalTable.class::cast ).sorted( Comparator.comparingLong( p -> p.id ) ).forEach( physicalTables::add );
        }
        String reason = null;
        if ( adapter.type != AdapterType.SOURCE || table.entityType != EntityType.SOURCE || table.modifiable ) {
            reason = "Only read-only imported source tables can be edited here.";
        } else if ( mapping.rules().stream().anyMatch( rule -> !supportsRuleInEditor( mapping, rule ) ) ) {
            reason = "This mapping uses rules that this editor does not support.";
        } else if ( allocations.stream().anyMatch( a -> a.adapterId != adapterId )
                || !adapter.adapterName.equalsIgnoreCase( "CSV" )
                || !(catalog.getAdapterCatalog( adapterId ).orElse( null ) instanceof RelAdapterCatalog) ) {
            reason = "Editing is currently supported for CSV source mappings on one adapter.";
        }
        String removalReason = SchemaMappingManager.removalBlockedReason( catalog, mapping );
        if ( removalReason == null && snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() == 1 && (!snapshot.rel().getConnectedViews( table.id ).isEmpty() || !snapshot.rel().getExportedKeys( table.id ).isEmpty()) ) {
            removalReason = "Dependent views or foreign keys prevent dropping this mapping's logical target.";
        }
        return new RelMappingState( adapter, mapping, namespace, table, columns, List.copyOf( physicalTables ),
                revision( adapter, mapping, namespace, table, columns, allocations, physicalTables ), reason,
                reason != null ? reason : removalReason, snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() > 1 ? table.id : null,
                snapshot.mappings().getMappingsForLogicalEntity( table.id ).size() > 1 ? snapshot.rel().getColumns( table.id ) : List.of() );
    }


    private static boolean supportsRuleInEditor( SchemaMapping mapping, MappingRule rule ) {
        if ( mapping.sourceModel() != DataModel.RELATIONAL || mapping.targetModel() != DataModel.RELATIONAL ) {
            return false;
        }
        return rule instanceof ColumnMappingRule && RuleRepository.definitionFor( rule.getClass() ).isPresent();
    }


    /**
     * Validates the proposed changes, target compatibility, and dependencies, then returns a normalized update command.
     */
    public RelMappingUpdate validate( RelMappingUpdate update, Snapshot snapshot ) {
        // checks that the save request refers to the same adapter and mapping that RelMappingState loaded
        if ( update.adapterId() != adapter.id || update.mappingId() != mapping.id() ) {
            throw MappingError.Missing.resource();
        }

        // prevents a stale editor from overwriting newer changes
        validateRevision( update.expectedRevision() );
        LogicalNamespace targetNamespace = update.targetNamespaceId() == null ? namespace
                : snapshot.getNamespace( update.targetNamespaceId() ).orElseThrow( () -> MappingError.Missing.targetNamespace() );
        if ( targetNamespace.dataModel != DataModel.RELATIONAL ) {
            throw MappingError.Unsupported.relationalNamespaceRequired();
        }
        if ( targetNamespace.hidden ) {
            throw MappingError.Validation.visibleNamespaceRequired();
        }
        Map<String, String> errors = new LinkedHashMap<>();
        String label = update.mappingName() == null ? "" : update.mappingName().trim();
        if ( label.isEmpty() || label.length() > SchemaMappingManager.MAPPING_NAME_MAX_LENGTH ) {
            errors.put( "mappingName", SchemaMappingManager.mappingNameMessage() );
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
            ColumnName normalized = normalizeColumnUpdate( column, requestedColumn, name, included, errors );
            logicalChanged |= !name.equals( column.name ) || included != isIncluded( column.id ) || normalized.changesTypeOrNullability();
            names.add( normalized );
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
                                && compatible( columns.stream().filter( sourceColumn -> sourceColumn.id == requestedColumn.logicalColumnId() ).findFirst()
                                        .orElseThrow( () -> MappingError.Missing.logicalColumn( mapping.id(), requestedColumn.logicalColumnId() ) ), targetColumn ) ) ) ) {
                    errors.put( "columns", "The included source fields must match all existing target columns in name, type and nullability." );
                }
            }
        }
        if ( !errors.isEmpty() ) {
            throw MappingError.Validation.fields( errors );
        }
        if ( logicalChanged && mapping.id() >= 0 ) {
            List<String> dependencies = new ArrayList<>( snapshot.rel().getConnectedViews( table.id ).stream().map( v -> v.name ).toList() );
            Set<Long> excluded = names.stream().filter( c -> !c.included() ).map( ColumnName::logicalColumnId ).collect( Collectors.toSet() );
            Set<Long> changedTypes = names.stream().filter( ColumnName::changesTypeOrNullability ).map( ColumnName::logicalColumnId ).collect( Collectors.toSet() );
            snapshot.rel().getExportedKeys( table.id ).stream()
                    .filter( key -> key.referencedKeyFieldIds.stream().anyMatch( id -> excluded.contains( id ) || changedTypes.contains( id ) ) )
                    .map( key -> key.name ).forEach( dependencies::add );
            if ( !changedTypes.isEmpty() ) {
                addColumnConversionDependencies( snapshot, names, changedTypes, dependencies );
            }
            if ( namespaceChanged ) {
                snapshot.rel().getExportedKeys( table.id ).stream().map( key -> key.name ).forEach( dependencies::add );
                snapshot.rel().getForeignKeys( table.id ).stream().map( key -> key.name ).forEach( dependencies::add );
            }
            if ( !dependencies.isEmpty() ) {
                throw MappingError.Conflict.schemaChangeBlocked( !changedTypes.isEmpty(), dependencies.stream().distinct().toList() );
            }
        }
        return new RelMappingUpdate( adapter.id, mapping.id(), revision, label, tableName, List.copyOf( names ), targetNamespace.id, update.existingTargetEntityId(), update.sourceAllocationId() );
    }


    /**
     * Resolves a proposed field using the same type and rule validation as a mapping update, without changing the catalog.
     */
    public LogicalColumn previewColumn( ColumnName requested ) {
        if ( requested == null || Boolean.FALSE.equals( requested.included() ) ) {
            throw MappingError.Validation.fields( Map.of( "columns", "Supply an included field to check." ) );
        }
        LogicalColumn column = columns.stream().filter( field -> field.id == requested.logicalColumnId() ).findFirst()
                .orElseThrow( () -> MappingError.Missing.logicalColumn( mapping.id(), requested.logicalColumnId() ) );
        Map<String, String> errors = new LinkedHashMap<>();
        ColumnName normalized = normalizeColumnUpdate( column, requested, column.name, true, errors );
        if ( !errors.isEmpty() ) {
            throw MappingError.Validation.fields( errors );
        }
        return column.toBuilder()
                .type( normalized.targetType() == null ? column.type : normalized.targetType() )
                .length( normalized.targetType() == null ? column.length : normalized.targetPrecision() )
                .scale( normalized.targetType() == null ? column.scale : normalized.targetScale() )
                .nullable( normalized.targetNullable() == null ? column.nullable : normalized.targetNullable() )
                .build();
    }


    /**
     * Returns a ColumnName ready for the mapping update.
     */
    private ColumnName normalizeColumnUpdate( LogicalColumn column, ColumnName requestedColumn, String name, boolean included, Map<String, String> errors ) {
        if ( requestedColumn == null ) {
            return new ColumnName( column.id, name, included );
        }
        PolyType type = Objects.requireNonNullElse( requestedColumn.targetType(), column.type );
        Boolean nullable = requestedColumn.targetNullable();
        if ( nullable != null && nullable == column.nullable ) {
            nullable = null;
        }
        Integer precision = requestedColumn.targetPrecision();
        Integer scale = requestedColumn.targetScale();
        boolean clearScale = type.allowsScale() && Objects.equals( scale, AlgDataType.SCALE_NOT_SPECIFIED );
        if ( Objects.equals( precision, AlgDataType.PRECISION_NOT_SPECIFIED ) && type.allowsPrec() ) {
            precision = null;
        } else if ( type != column.type && precision == null ) {
            var source = physicalColumns( column.id ).filter( field -> field.type == type ).findFirst().orElse( null );
            if ( source != null ) {
                precision = source.length;
                scale = scale == null ? source.scale : scale;
            }
        } else if ( type == column.type ) {
            precision = precision == null ? column.length : precision;
            scale = scale == null ? column.scale : scale;
        }
        if ( clearScale ) {
            scale = null;
        }
        if ( scale != null && precision == null && type.allowsScale() ) {
            precision = AlgDataTypeSystem.DEFAULT.getDefaultPrecision( type );
        }
        boolean typeChanged = type != column.type || !Objects.equals( precision, column.length ) || !Objects.equals( scale, column.scale );
        String ruleType = requestedColumn.ruleType();
        if ( ruleType == null && included && !isIncluded( column.id ) ) {
            ruleType = mapping.excludedColumnRules().get( column.id );
        }
        ColumnName normalized = typeChanged
                ? new ColumnName( column.id, name, included, type, nullable, precision, scale, ruleType )
                : new ColumnName( column.id, name, included, null, nullable, null, null, ruleType );
        if ( normalized.changesTypeOrNullability() || normalized.ruleType() != null ) {
            validateColumnConversion( column, normalized, errors );
        }
        return normalized;
    }


    /**
     * Checks whether a requested type or nullability change is allowed for a column:
     * - allows changes for included scalar columns with an independently editable logical target
     * - checks supported conversions
     * If validation fails, it adds a field-specific error under columns.<id>
     */
    private void validateColumnConversion( LogicalColumn column, ColumnName update, Map<String, String> errors ) {
        String field = "columns." + column.id;
        PolyType type = update.targetType();
        // shared or fixed target || column excluded || collection valued column
        // If all conditions are false, the column is an included scalar field in an independently editable target,
        // and validation proceeds to check whether the requested type conversion is supported.
        if ( !update.included() || !canConfigureColumn( column ) ) {
            errors.put( field, "Rule, type and nullability changes require an included scalar field in an editable, independent target." );
            return;
        }
        if ( type != null && !validTypeParameters( type, update.targetPrecision(), update.targetScale() ) ) {
            errors.put( field, "Invalid precision, length or scale for %s.".formatted( type ) );
            return;
        }
        if ( update.ruleType() != null ) {
            var rule = RuleRepository.definitionForId( update.ruleType() ).orElse( null );
            if ( rule == null || (rule.ruleClass() != RelCopyColumnMappingRule.class && rule.ruleClass() != RelConvertColumnMappingRule.class) ) {
                errors.put( field, "Unsupported column mapping rule '%s'.".formatted( update.ruleType() ) );
                return;
            }
            if ( rule.ruleClass() == RelCopyColumnMappingRule.class ) {
                var target = type == null ? column : column.toBuilder().type( type ).length( update.targetPrecision() ).scale( update.targetScale() ).build();
                if ( physicalColumns( column.id ).anyMatch( source -> !SchemaMappingManager.hasMatchingValueType( target, source ) ) ) {
                    errors.put( field, "Copy requires the source type, length and scale. Choose Convert to change the type." );
                }
                return;
            }
        }
        var factory = AlgDataTypeFactory.DEFAULT;
        var targetType = type == null ? column.getAlgDataType( factory )
                : LogicalColumn.getAlgDataType( factory, update.targetPrecision(), update.targetScale(), type,
                        null, null, null, column.nullable, column.elementsNullable );
        physicalColumns( column.id ).filter( source -> !ScalarConversions.supports( source.getAlgDataType( factory ), targetType ) ).findFirst()
                .ifPresent( source -> errors.put( field, "Conversion from %s to %s is not supported.".formatted( source.type, targetType.getPolyType() ) ) );
    }


    /**
     * returns the list of target types offered in the mapping editor for that column.
     */
    public List<PolyType> targetTypeOptions( LogicalColumn column ) {
        if ( !canConfigureColumn( column ) ) {
            return List.of();
        }
        return physicalColumns( column.id ).findFirst()
                .map( source -> ScalarConversions.targetTypes( source.getAlgDataType( AlgDataTypeFactory.DEFAULT ) ) ).orElse( List.of() );
    }


    private boolean canConfigureColumn( LogicalColumn column ) {
        return editable() && fixedTargetEntityId == null && physicalTables.size() == 1 && column.collectionsType == null
                && mapping.sourceModel() == DataModel.RELATIONAL && mapping.targetModel() == DataModel.RELATIONAL;
    }


    private Stream<PhysicalColumn> physicalColumns( long columnId ) {
        return physicalTables.stream().flatMap( physical -> physical.columns.stream() ).filter( column -> column.id == columnId );
    }


    private static boolean validTypeParameters( PolyType type, Integer precision, Integer scale ) {
        var system = AlgDataTypeSystem.DEFAULT;
        if ( precision != null ) {
            int minimum = type == PolyType.TIME || type == PolyType.TIMESTAMP ? 0 : 1;
            int maximum = system.getMaxPrecision( type );
            if ( !type.allowsPrec() || precision < minimum || (maximum >= 0 && precision > maximum) ) {
                return false;
            }
        }
        return scale == null || (type.allowsScale() && scale >= 0
                && scale <= Objects.requireNonNullElseGet( precision, () -> system.getDefaultPrecision( type ) )
                && scale <= system.getMaxScale( type ));
    }


    /**
     * Checks whether changing a column’s type or nullability in a saved mapping would affect objects that depend on the logical table
     */
    private void addColumnConversionDependencies( Snapshot snapshot, List<ColumnName> updates, Set<Long> changedColumns, List<String> dependencies ) {
        snapshot.rel().getForeignKeys( table.id ).stream().filter( key -> key.fieldIds.stream().anyMatch( changedColumns::contains ) )
                .map( key -> key.name ).forEach( dependencies::add );
        snapshot.rel().getConstraints( table.id ).stream().filter( constraint -> constraint.key.fieldIds.stream().anyMatch( changedColumns::contains ) )
                .map( constraint -> constraint.name ).forEach( dependencies::add );
        snapshot.rel().getIndexes( table.id, false ).stream().filter( index -> index.key.fieldIds.stream().anyMatch( changedColumns::contains ) )
                .map( index -> index.name ).forEach( dependencies::add );
        snapshot.rel().getPrimaryKeys().stream().filter( key -> key.entityId == table.id )
                .filter( key -> updates.stream().anyMatch( update -> key.fieldIds.contains( update.logicalColumnId() )
                        && (update.targetType() != null || Boolean.TRUE.equals( update.targetNullable() )) ) )
                .forEach( key -> dependencies.add( "Primary key of " + table.name ) );
        snapshot.rel().getTables( (Pattern) null, null ).stream().filter( target -> Objects.equals( target.synchronizedSourceEntityId, table.id ) )
                .map( target -> target.name ).forEach( dependencies::add );
        columns.stream().filter( column -> column.defaultValue != null )
                .filter( column -> updates.stream().anyMatch( update -> update.logicalColumnId() == column.id && update.targetType() != null ) )
                .map( column -> "Default value of " + column.name ).forEach( dependencies::add );
    }


    /**
     * Rejects editing when unsupported or when the submitted revision differs from the current state.
     */
    public void validateRevision( String expectedRevision ) {
        if ( !editable() ) {
            throw MappingError.Unsupported.editor( readOnlyReason );
        }
        if ( !Objects.equals( revision, expectedRevision ) ) {
            throw MappingError.Revision.stale();
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
        if ( name.length() > SchemaMappingManager.IDENTIFIER_MAX_LENGTH || !name.matches( SchemaMappingManager.IDENTIFIER_PATTERN ) ) {
            errors.put( field, SchemaMappingManager.identifierMessage() );
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
            mapping.excludedColumnRules().entrySet().stream().sorted( Map.Entry.comparingByKey() )
                    .forEach( entry -> hash( digest, entry.getKey(), entry.getValue() ) );
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
            throw MappingError.Unsupported.existingSourceTargetRequired();
        }
        if ( catalog.getSnapshot().alloc().getFromLogical( target.id ).stream().map( allocation -> allocation.partitionId ).distinct().count() != 1 ) {
            throw MappingError.Unsupported.existingTargetPartitionRequired();
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


}

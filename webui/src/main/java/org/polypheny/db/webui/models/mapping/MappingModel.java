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

package org.polypheny.db.webui.models.mapping;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.algebra.mapping.RuleRepository;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RuleDefinition;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.DocumentMappingState;
import org.polypheny.db.catalog.mapping.RelMappingState;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.type.PolyType;

/**
 * MappingModel defines the mapping data the backend sends to the frontend:
 * Summary - brief mapping information used by adapter cards and the sidebar.
 * ListResponse - groups summaries by adapter.
 * RelDetail - supplies the editor with names, fields, physical references, rules, editability, revision and validation guidance.
 * Error - supplies error messages, field errors and blocking dependencies.
 */
@Slf4j
public final class MappingModel {

    private MappingModel() {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RelConversionValidation( List<ColumnCheck> columns ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record ColumnCheck( long logicalColumnId, int checkedRecords, List<ConversionFailure> errors ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record ConversionFailure( int recordNumber, Error error ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Summary( long mappingId, long adapterId, String adapterName, String mappingName,
                           String namespaceName, String logicalTableName, boolean editable, String readOnlyReason,
                           MappingKind kind, Long sourceEntityId, long targetEntityId, DataModel sourceModel, DataModel targetModel,
                           List<PhysicalSource> physicalSources, String revision, boolean removable, String removalBlockedReason, boolean removesTarget ) {

        public static Summary from( DocumentMappingState state ) {
            var p = state.physical();
            return new Summary( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().name, state.collection().name, state.editable(), state.readOnlyReason(),
                    state.mapping().kind(), null, state.collection().id, DataModel.DOCUMENT, DataModel.DOCUMENT,
                    List.of( new PhysicalSource( p.allocationId, p.id, p.namespaceName, p.name ) ), state.revision(), state.removable(), state.removalBlockedReason(), true );
        }

        public static Summary from( RelMappingState state ) {
            return new Summary( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().name, state.table().name, state.editable(), state.readOnlyReason(),
                    state.mapping().kind(), state.mapping().sourceEntityId(), state.mapping().targetEntityId(), state.mapping().sourceModel(), state.mapping().targetModel(),
                    state.physicalTables().stream().map( p -> new PhysicalSource( p.allocationId, p.id, p.namespaceName, p.name ) ).toList(), state.revision(),
                    state.removable(), state.removalBlockedReason(), state.fixedTargetEntityId() == null );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record AdapterMappings( long adapterId, String adapterName, List<Summary> mappings ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record ListResponse( List<AdapterMappings> adapters ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record OverviewResponse( List<Summary> mappings ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RelSource( long adapterId, String adapterName, long targetEntityId, String namespaceName, String logicalTableName, MappingKind kind, long sourceAllocationId ) {

        public static RelSource from( RelMappingState state ) {
            var physical = state.physicalTables().get( 0 );
            return new RelSource( state.adapter().id, state.adapter().uniqueName, state.table().id, physical.namespaceName, physical.name, state.mapping().kind(), physical.allocationId );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RelSourcesResponse( List<RelSource> sources ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record PhysicalSource( long allocationId, long physicalEntityId, String namespaceName, String physicalName ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record PhysicalColumnReference( long allocationId, long physicalEntityId, long physicalColumnId, String physicalName, int physicalPosition,
                                           String typeDisplay, boolean nullable, PolyType type, Integer precision, Integer scale, boolean primary ) {
    }


    private record SourceTable( long adapterId, String schema, String table ) {

        Set<String> primaryColumns( Catalog catalog, Snapshot snapshot ) {
            return exportedColumns().map( columns -> columns.stream().filter( ExportedColumn::primary )
                    .map( ExportedColumn::physicalColumnName ).collect( Collectors.toSet() ) )
                    .orElseGet( () -> persistedPrimaryColumns( catalog, snapshot ) );
        }


        private Optional<List<ExportedColumn>> exportedColumns() {
            try {
                return AdapterManager.getInstance().getSource( adapterId ).filter( DataSource::supportsRelational )
                        .flatMap( source -> matchingExport( source.asRelationalDataSource().getExportedColumnsFresh() ) );
            } catch ( RuntimeException e ) {
                log.debug( "Could not read primary key metadata for source {}.{} on adapter {}", schema, table, adapterId, e );
                return Optional.empty();
            }
        }


        private Optional<List<ExportedColumn>> matchingExport( Map<String, List<ExportedColumn>> exports ) {
            var matchingTables = exports.entrySet().stream().filter( entry -> entry.getValue() != null && !entry.getValue().isEmpty() )
                    .filter( entry -> table.equals( entry.getKey() ) || table.equals( entry.getValue().get( 0 ).physicalTableName() ) )
                    .map( Map.Entry::getValue ).toList();
            var matchingSchemas = matchingTables.stream().filter( columns -> Objects.equals( schema, columns.get( 0 ).physicalSchemaName() ) ).toList();
            // CSV exports are keyed by table name. Their schema value is the file name, while existing allocations can use the logical namespace.
            var candidates = matchingSchemas.isEmpty() ? matchingTables : matchingSchemas;
            return candidates.size() == 1 ? Optional.of( candidates.get( 0 ) ) : Optional.empty();
        }


        /**
         * reads the source key saved in PhysicalTable.uniqueFieldIds
         * and resolves those IDs to source column names
         */
        private Set<String> persistedPrimaryColumns( Catalog catalog, Snapshot snapshot ) {
            // The first allocation of an imported table identifies its source.
            return catalog.getAdapterCatalog( adapterId ).stream().flatMap( adapter -> adapter.getPhysicals().values().stream() )
                    .filter( PhysicalTable.class::isInstance ).map( PhysicalTable.class::cast )
                    .filter( physical -> Objects.equals( schema, physical.namespaceName ) && table.equals( physical.name ) )
                    .filter( physical -> snapshot.alloc().getFromLogical( physical.logicalId ).stream()
                            .noneMatch( allocation -> allocation.id < physical.allocationId ) )
                    .min( Comparator.comparingLong( physical -> physical.allocationId ) )
                    .map( physical -> {
                        List<Long> sourceIds = physical.getPrimaryColumns();
                        return physical.columns.stream().filter( column -> sourceIds.contains( column.id ) )
                                .map( column -> column.name ).collect( Collectors.toSet() );
                    } ).orElseGet( Set::of );
        }
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Column( long logicalColumnId, String logicalName, int logicalPosition, String typeDisplay, boolean nullable,
                          List<PhysicalColumnReference> physicalReferences, boolean included, PolyType type, List<PolyType> targetTypeOptions,
                          Integer precision, Integer scale ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record TypeParameters( Integer minPrecision, Integer maxPrecision, Integer defaultPrecision, Integer maxScale, Integer defaultScale ) {

        static TypeParameters from( PolyType type ) {
            var system = AlgDataTypeSystem.DEFAULT;
            int defaultPrecision = system.getDefaultPrecision( type );
            int maxPrecision = system.getMaxPrecision( type );
            int minPrecision = type == PolyType.TIME || type == PolyType.TIMESTAMP ? 0 : 1;
            return new TypeParameters( minPrecision, maxPrecision < 0 ? null : maxPrecision, defaultPrecision < 0 ? null : defaultPrecision,
                    type.allowsScale() ? system.getMaxScale( type ) : null, type.allowsScale() ? type.getDefaultScale() : null );
        }
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RuleOption( String type, String label ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Rule( String type, Long targetLogicalColumnId ) {

        public static Rule from( MappingRule rule ) {
            String type = RuleRepository.definitionFor( rule.getClass() ).map( RuleDefinition::id ).orElseGet( () -> rule.getClass().getSimpleName() );
            return new Rule( type,
                    rule instanceof ColumnMappingRule columnRule ? columnRule.targetLogicalColumnId() : null );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Validation( int mappingNameMaxLength, int identifierMaxLength, String identifierPattern,
                              String mappingNameMessage, String identifierMessage ) {

        public static Validation current() {
            return new Validation( SchemaMappingManager.MAPPING_NAME_MAX_LENGTH, SchemaMappingManager.IDENTIFIER_MAX_LENGTH,
                    SchemaMappingManager.IDENTIFIER_PATTERN, SchemaMappingManager.mappingNameMessage(), SchemaMappingManager.identifierMessage() );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RelDetail( long mappingId, long adapterId, String adapterName, String mappingName,
                          String namespaceName, String logicalTableName, boolean editable, String readOnlyReason,
                          String revision, long namespaceId, boolean namespaceCaseSensitive, List<Column> columns,
                          List<PhysicalSource> physicalSources, MappingKind kind, Long sourceEntityId, long targetEntityId,
                          DataModel sourceModel, DataModel targetModel, List<Rule> rules, Validation validation, List<RuleOption> availableRules,
                          boolean removable, String removalBlockedReason, boolean removesTarget, Long fixedTargetEntityId, List<Column> targetColumns,
                          Map<PolyType, TypeParameters> typeParameters, Map<Long, String> excludedColumnRules ) {

        public static RelDetail from( RelMappingState state, Catalog catalog ) {
            var snapshot = catalog.getSnapshot();
            Map<SourceTable, Set<String>> primaryColumns = new HashMap<>();
            Map<Long, Set<String>> primaryColumnsByPhysicalId = new HashMap<>();
            for ( var physical : state.physicalTables() ) {
                var source = new SourceTable( physical.adapterId, physical.namespaceName, physical.name );
                primaryColumnsByPhysicalId.put( physical.id, primaryColumns.computeIfAbsent( source, key -> key.primaryColumns( catalog, snapshot ) ) );
            }
            List<Column> columns = state.columns().stream().map( column -> new Column(
                    column.id, column.name, column.position,
                    AlgDataTypeFactory.DEFAULT.createTypeWithNullability( column.getAlgDataType( AlgDataTypeFactory.DEFAULT ), true ).getFullTypeString(),
                    column.nullable,
                    state.physicalTables().stream().flatMap( physical -> physical.columns.stream().filter( c -> c.id == column.id )
                            .map( c -> new PhysicalColumnReference( physical.allocationId, physical.id, c.id, c.name, c.position,
                                    c.getAlgDataType( AlgDataTypeFactory.DEFAULT ).getFullTypeString(), c.nullable, c.type, c.length, c.scale,
                                    primaryColumnsByPhysicalId.get( physical.id ).contains( c.name ) ) ) ).toList(),
                    state.isIncluded( column.id ), column.type, state.targetTypeOptions( column ), column.length, column.scale ) ).toList();
            Map<PolyType, TypeParameters> typeParameters = columns.stream().flatMap( column -> column.targetTypeOptions().stream() )
                    .filter( PolyType::allowsPrec ).distinct()
                    .collect( Collectors.toMap( type -> type, TypeParameters::from ) );
            return new RelDetail( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().name, state.table().name, state.editable(), state.readOnlyReason(), state.revision(),
                    state.namespace().id, state.namespace().caseSensitive, columns,
                    state.physicalTables().stream().map( p -> new PhysicalSource( p.allocationId, p.id, p.namespaceName, p.name ) ).toList(),
                    state.mapping().kind(), state.mapping().sourceEntityId(), state.mapping().targetEntityId(), state.mapping().sourceModel(), state.mapping().targetModel(),
                    state.mapping().rules().stream().map( Rule::from ).toList(), Validation.current(),
                    RuleRepository.definitions().stream().filter( definition -> ColumnMappingRule.class.isAssignableFrom( definition.ruleClass() ) )
                            .map( definition -> new RuleOption( definition.id(), definition.label() ) ).toList(),
                    state.removable(), state.removalBlockedReason(),
                    state.fixedTargetEntityId() == null, state.fixedTargetEntityId(),
                    state.targetColumns().stream().map( column -> new Column(
                            column.id, column.name, column.position, AlgDataTypeFactory.DEFAULT.createTypeWithNullability( column.getAlgDataType( AlgDataTypeFactory.DEFAULT ), true ).getFullTypeString(),
                            column.nullable, List.of(), true, column.type, List.of(), column.length, column.scale ) ).toList(),
                    typeParameters, state.mapping().excludedColumnRules() );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Error( String code, String message, Map<String, String> fieldErrors, List<String> blockingEntities ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record DocumentDetail( long mappingId, long adapterId, String adapterName, String mappingName, long namespaceId, String namespaceName,
                                  boolean namespaceCaseSensitive, String logicalCollectionName, long targetEntityId, long sourceAllocationId,
                                  DataModel sourceModel, DataModel targetModel, MappingKind kind, List<PhysicalSource> physicalSources,
                                  List<Rule> rules, String revision, boolean editable, String readOnlyReason,
                                  boolean removable, String removalBlockedReason, Validation validation ) {

        public static DocumentDetail from( DocumentMappingState state ) {
            var p = state.physical();
            return new DocumentDetail( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().id, state.namespace().name, state.namespace().caseSensitive, state.collection().name,
                    state.mapping().targetEntityId(), p.allocationId, DataModel.DOCUMENT, DataModel.DOCUMENT, state.mapping().kind(),
                    List.of( new PhysicalSource( p.allocationId, p.id, p.namespaceName, p.name ) ),
                    state.mapping().rules().stream().map( Rule::from ).toList(), state.revision(), state.editable(), state.readOnlyReason(),
                    state.removable(), state.removalBlockedReason(), Validation.current() );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record DocumentSource( long adapterId, String adapterName, long sourceEntityId, long sourceAllocationId,
                                  String namespaceName, String physicalName ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record DocumentSourcesResponse( List<DocumentSource> sources ) {
    }

}

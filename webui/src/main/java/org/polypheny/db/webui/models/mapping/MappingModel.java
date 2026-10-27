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
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.MappingState;

/**
 * MappingModel defines the mapping data the backend sends to the frontend:
 * Summary - brief mapping information used by adapter cards and the sidebar.
 * ListResponse - groups summaries by adapter.
 * Detail - supplies the editor with names, fields, physical references, rules, editability, revision and validation guidance.
 * Error - supplies error messages, field errors and blocking dependencies.
 */
public final class MappingModel {

    private MappingModel() {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Summary( long mappingId, long adapterId, String adapterName, String mappingName,
                           String namespaceName, String logicalTableName, boolean editable, String readOnlyReason,
                           MappingKind kind, Long sourceEntityId, long targetEntityId, DataModel sourceModel, DataModel targetModel ) {

        public static Summary from( MappingState state ) {
            return new Summary( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().name, state.table().name, state.editable(), state.readOnlyReason(),
                    state.mapping().kind(), state.mapping().sourceEntityId(), state.mapping().targetEntityId(), state.mapping().sourceModel(), state.mapping().targetModel() );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record AdapterMappings( long adapterId, String adapterName, List<Summary> mappings ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record ListResponse( List<AdapterMappings> adapters ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record PhysicalSource( long allocationId, long physicalEntityId, String namespaceName, String physicalName ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record PhysicalColumnReference( long allocationId, long physicalEntityId, long physicalColumnId, String physicalName, int physicalPosition,
                                           String typeDisplay, boolean nullable ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Column( long logicalColumnId, String logicalName, int logicalPosition, String typeDisplay, boolean nullable,
                          List<PhysicalColumnReference> physicalReferences, boolean included ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record RuleOption( String type, String label ) {
    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Rule( String type, Long targetLogicalColumnId ) {

        public static Rule from( MappingRule rule ) {
            return new Rule( rule instanceof DirectColumnMappingRule ? "DIRECT_COLUMN" : rule.getClass().getSimpleName(),
                    rule instanceof ColumnMappingRule columnRule ? columnRule.targetLogicalColumnId() : null );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Validation( int mappingNameMaxLength, int identifierMaxLength, String identifierPattern,
                              String mappingNameMessage, String identifierMessage ) {

        public static Validation current() {
            return new Validation( MappingState.MAPPING_NAME_MAX_LENGTH, MappingState.IDENTIFIER_MAX_LENGTH,
                    MappingState.IDENTIFIER_PATTERN, MappingState.mappingNameMessage(), MappingState.identifierMessage() );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Detail( long mappingId, long adapterId, String adapterName, String mappingName,
                          String namespaceName, String logicalTableName, boolean editable, String readOnlyReason,
                          String revision, long namespaceId, boolean namespaceCaseSensitive, List<Column> columns,
                          List<PhysicalSource> physicalSources, MappingKind kind, Long sourceEntityId, long targetEntityId,
                          DataModel sourceModel, DataModel targetModel, List<Rule> rules, Validation validation, List<RuleOption> availableRules ) {

        public static Detail from( MappingState state ) {
            List<Column> columns = state.columns().stream().map( column -> new Column(
                    column.id, column.name, column.position,
                    AlgDataTypeFactory.DEFAULT.createTypeWithNullability( column.getAlgDataType( AlgDataTypeFactory.DEFAULT ), true ).getFullTypeString(),
                    column.nullable,
                    state.physicalTables().stream().flatMap( physical -> physical.columns.stream().filter( c -> c.id == column.id )
                            .map( c -> new PhysicalColumnReference( physical.allocationId, physical.id, c.id, c.name, c.position,
                                    c.getAlgDataType( AlgDataTypeFactory.DEFAULT ).getFullTypeString(), c.nullable ) ) ).toList(), state.isIncluded( column.id ) ) ).toList();
            return new Detail( state.mapping().id(), state.adapter().id, state.adapter().uniqueName, state.mapping().name(),
                    state.namespace().name, state.table().name, state.editable(), state.readOnlyReason(), state.revision(),
                    state.namespace().id, state.namespace().caseSensitive, columns,
                    state.physicalTables().stream().map( p -> new PhysicalSource( p.allocationId, p.id, p.namespaceName, p.name ) ).toList(),
                    state.mapping().kind(), state.mapping().sourceEntityId(), state.mapping().targetEntityId(), state.mapping().sourceModel(), state.mapping().targetModel(),
                    state.mapping().rules().stream().map( Rule::from ).toList(), Validation.current(),
                    List.of( new RuleOption( "DIRECT_COLUMN", "Copy source value" ) ) );
        }

    }


    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public record Error( String code, String message, Map<String, String> fieldErrors, List<String> blockingEntities ) {
    }

}

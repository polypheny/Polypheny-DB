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

package org.polypheny.db.algebra.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.rex.RexBuilder;


/**
 * The context resolves a source field by column ID within the selected allocation’s input.
 * It uses:
 * - Allocation metadata from RelMappingBinding to validate the column.
 * - The actual input schema to determine the column’s position and type.
 */
public final class RelFieldMappingContext {

    @Getter
    private final SchemaMapping mapping;
    @Getter
    private final RelMappingBinding binding;
    @Getter
    private final RexBuilder rexBuilder;
    private final Map<Long, AlgDataTypeField> inputFields;
    private final Snapshot snapshot;


    public RelFieldMappingContext( SchemaMapping mapping, RelMappingBinding binding, AlgDataType inputType, RexBuilder rexBuilder, Snapshot snapshot ) {
        binding.validate( mapping );
        this.mapping = mapping;
        this.binding = binding;
        this.rexBuilder = rexBuilder;
        this.snapshot = snapshot;
        Map<Long, AlgDataTypeField> fields = new HashMap<>();
        for ( AlgDataTypeField field : inputType.getFields() ) {
            if ( field.getId() == null ) {
                throw MappingError.Missing.fieldId( mapping.id(), field.getName(), binding.allocationId() );
            }
            if ( fields.putIfAbsent( field.getId(), field ) != null ) {
                throw MappingError.Duplicate.inputFieldId( mapping.id(), field.getId(), binding.allocationId() );
            }
        }
        inputFields = Map.copyOf( fields );
    }


    public String getTargetTableName() {
        Objects.requireNonNull( snapshot, "Mapping conversions require a catalog snapshot." );
        var table = snapshot.rel().getTable( mapping.targetEntityId() )
                .orElseThrow( () -> MappingError.Missing.target( mapping.id(), mapping.targetEntityId() ) );
        return snapshot.getNamespace( table.namespaceId )
                .orElseThrow( () -> MappingError.Missing.namespace( mapping.id(), table.namespaceId ) ).name + "." + table.name;
    }

    /**
     * Finds the requested column in the actual input
     */
    public AlgDataTypeField resolveSourceField( long columnId ) {
        PhysicalColumn registered = binding.columns().get( columnId );
        AlgDataTypeField field = inputFields.get( columnId );
        if ( registered == null || field == null ) {
            throw MappingError.Missing.sourceField( mapping.id(), columnId, binding.allocationId() );
        }
        if ( !Objects.equals( registered.logicalName, field.getName() )
                || !registered.getAlgDataType( rexBuilder.getTypeFactory() ).equals( field.getType() ) ) {
            throw MappingError.Algebra.sourceFieldMismatch( mapping.id(), columnId, binding.allocationId() );
        }
        return field;
    }


    /**
     * Checks adapter field names before the input is registered with the planner.
     */
    public void validatePhysicalNames() {
        for ( AlgDataTypeField field : inputFields.values() ) {
            PhysicalColumn registered = binding.columns().get( field.getId() );
            if ( registered != null && !Objects.equals( registered.name, field.getPhysicalName() ) ) {
                throw MappingError.Algebra.physicalNameMismatch( mapping.id(), field.getPhysicalName(), registered.name, field.getId(), binding.allocationId() );
            }
        }
    }

}

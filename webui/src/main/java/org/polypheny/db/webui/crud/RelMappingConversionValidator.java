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

package org.polypheny.db.webui.crud;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.mapping.RelConversionColumnTransformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.exceptions.MappingException;
import org.polypheny.db.catalog.exceptions.MappingExecutionException;
import org.polypheny.db.catalog.mapping.RelMappingState;
import org.polypheny.db.ddl.RelMappingUpdate.ColumnName;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.mapping.MappingModel;


final class RelMappingConversionValidator {

    static final int SAMPLE_SIZE = 10;


    private RelMappingConversionValidator() {
    }


    static MappingModel.RelConversionValidation validate( RelMappingState state, List<ColumnName> requested, Statement statement ) {
        if ( requested == null || requested.isEmpty() || requested.stream().anyMatch( column -> column == null )
                || new HashSet<>( requested.stream().map( ColumnName::logicalColumnId ).toList() ).size() != requested.size() ) {
            throw MappingError.Validation.fields( Map.of( "columns", "Supply unique included fields to check." ) );
        }
        var targets = requested.stream().map( state::previewColumn ).toList();
        if ( state.physicalTables().size() != 1 ) {
            throw MappingError.Unsupported.singlePhysicalSourceRequired();
        }
        var physical = state.physicalTables().get( 0 );
        var adapter = AdapterManager.getInstance().getAdapter( physical.adapterId )
                .orElseThrow( () -> MappingError.Missing.adapter( physical.adapterId ) );
        try {
            // Scan the original source, bypassing the saved mapping and any failing conversions in it.
            AlgBuilder builder = AlgBuilder.create( statement );
            var scan = adapter.getRelScan( physical.allocationId, builder );
            var sample = builder.push( scan ).limit( 0, SAMPLE_SIZE ).build();
            var implementation = statement.getQueryProcessor().prepareQuery( AlgRoot.of( sample, Kind.SELECT ), true, false );
            try ( var iterator = implementation.execute( statement, SAMPLE_SIZE ) ) {
                var rows = iterator.getNextBatch( SAMPLE_SIZE );
                var checks = targets.stream().map( target -> {
                    var source = scan.getTupleType().getFields().stream().filter( field -> field.getId() != null && field.getId() == target.id )
                            .findFirst().orElseThrow( () -> MappingError.Missing.logicalColumn( state.mapping().id(), target.id ) );
                    return checkColumn( state, target, source.getType(), source.getIndex(), rows, statement.getDataContext() );
                } ).toList();
                return new MappingModel.RelConversionValidation( checks );
            }
        } catch ( RuntimeException e ) {
            if ( MappingException.findCause( e ).isPresent() ) {
                throw e;
            }
            throw MappingError.Operation.scanFailed( state.mapping().name(), physical.adapterId, e );
        }
    }


    static MappingModel.ColumnCheck checkColumn( RelMappingState state, LogicalColumn target, AlgDataType sourceType,
            int sourceIndex, List<List<PolyValue>> rows, DataContext context ) {
        var factory = context.getTypeFactory();
        // This preview checks value conversion only; target NULL constraints belong to execution.
        var targetType = factory.createTypeWithNullability( target.getAlgDataType( factory ), true );
        List<MappingModel.ConversionFailure> errors = new ArrayList<>();
        int count = Math.min( rows.size(), SAMPLE_SIZE );
        for ( int i = 0; i < count; i++ ) {
            try {
                RelConversionColumnTransformer.convert( rows.get( i ).get( sourceIndex ), PolyString.of( target.name ),
                        state.mapping().name(), state.namespace().name + "." + state.table().name, sourceType, targetType, context );
            } catch ( MappingExecutionException error ) {
                errors.add( new MappingModel.ConversionFailure( i + 1,
                        new MappingModel.Error( error.getCode(), error.getMessage(), error.fieldErrors, error.blockingEntities ) ) );
            }
        }
        return new MappingModel.ColumnCheck( target.id, count, List.copyOf( errors ) );
    }

}

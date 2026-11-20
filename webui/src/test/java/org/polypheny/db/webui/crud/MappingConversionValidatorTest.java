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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.MappingValidationException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.mapping.MappingState;
import org.polypheny.db.ddl.MappingUpdate.ColumnName;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.models.mapping.MappingModel;
import org.polypheny.db.webui.models.mapping.MappingModel.ConversionFailure;


class MappingConversionValidatorTest {

    @BeforeAll
    static void initializeHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }

    private final JavaTypeFactoryImpl factory = new JavaTypeFactoryImpl();
    private final DataContext context = (DataContext) Proxy.newProxyInstance( DataContext.class.getClassLoader(), new Class[]{ DataContext.class },
            ( proxy, method, args ) -> method.getName().equals( "getTypeFactory" ) ? factory : null );
    private final LogicalColumn column = new LogicalColumn( 101, "amount", 50, 1, 1, PolyType.VARCHAR,
            null, 40, null, null, null, true, true, null, null );


    private MappingState state() {
        var physical = new PhysicalColumn( "source_amount", 50, 60, 7, 0, column );
        return new MappingState( new LogicalAdapter( 7, "csv_source", "CSV", AdapterType.SOURCE, DeployMode.EMBEDDED, Map.of() ),
                new SchemaMapping( 31, "Import", MappingKind.PHYSICAL_TO_LOGICAL, null, 50, DataModel.RELATIONAL, DataModel.RELATIONAL,
                        List.of( new DirectColumnMappingRule( column.id ) ), 60L ),
                new LogicalNamespace( 1, "public", DataModel.RELATIONAL, false, false ),
                new LogicalTable( 50, "amounts", 1, EntityType.SOURCE, null, false ), List.of( column ),
                List.of( new PhysicalTable( 70, 60, 50, "amounts.csv", List.of( physical ), 1, "files", List.of(), 7 ) ), "original", null, null );
    }


    @Test
    void serializesValidationResultsWithTheHttpServerMapper() throws Exception {
        var failure = new MappingModel.ConversionFailure( 3,
                new MappingModel.Error( "EXECUTION", "Cannot convert the source value to INTEGER.", Map.of(), List.of() ) );
        var response = new MappingModel.ConversionValidation( List.of(
                new MappingModel.ColumnCheck( column.id, 10, List.of( failure ) ),
                new MappingModel.ColumnCheck( 102, 4, List.of() ) ) );
        var json = HttpServer.mapper.readTree( HttpServer.mapper.writeValueAsString( response ) );
        assertEquals( 2, json.path( "columns" ).size() );
        assertEquals( column.id, json.at( "/columns/0/logicalColumnId" ).asLong() );
        assertEquals( 10, json.at( "/columns/0/checkedRecords" ).asInt() );
        assertEquals( 3, json.at( "/columns/0/errors/0/recordNumber" ).asInt() );
        assertEquals( "EXECUTION", json.at( "/columns/0/errors/0/error/code" ).asText() );
        assertEquals( failure.error().message(), json.at( "/columns/0/errors/0/error/message" ).asText() );
        assertTrue( json.at( "/columns/1/errors" ).isArray() );
        assertTrue( json.at( "/columns/1/errors" ).isEmpty() );
    }


    @Test
    void collectsAllFailuresAndChecksOnlyTenRecords() {
        var state = state();
        var target = state.previewColumn( new ColumnName( column.id, column.name, true, PolyType.INTEGER, true, null, null, "CONVERT_COLUMN" ) );
        List<List<PolyValue>> rows = IntStream.rangeClosed( 1, 11 ).mapToObj( i -> List.<PolyValue>of(
                PolyString.of( i == 3 || i == 7 || i == 11 ? "bad" + i : String.valueOf( i ) ) ) ).toList();
        var result = MappingConversionValidator.checkColumn( state, target, column.getAlgDataType( factory ), 0, rows, context );
        assertEquals( 10, result.checkedRecords() );
        assertEquals( List.of( 3, 7 ), result.errors().stream().map( ConversionFailure::recordNumber ).toList() );
        assertTrue( result.errors().get( 0 ).error().message().contains( "bad3" ) );
        assertTrue( result.errors().get( 0 ).error().message().contains( "INTEGER" ) );
        assertEquals( "EXECUTION", result.errors().get( 0 ).error().code() );
        assertEquals( PolyType.VARCHAR, state.columns().get( 0 ).type );
    }


    @Test
    void checksPrecisionWithoutApplyingTargetNullability() {
        var state = state();
        var target = state.previewColumn( new ColumnName( column.id, column.name, true, PolyType.DECIMAL, false, 3, 1, "CONVERT_COLUMN" ) );
        List<List<PolyValue>> rows = List.of( Collections.singletonList( null ), List.of( PolyString.of( "1234.5" ) ), List.of( PolyString.of( "1.2" ) ) );
        var result = MappingConversionValidator.checkColumn( state, target, column.getAlgDataType( factory ), 0, rows, context );
        assertEquals( 3, result.checkedRecords() );
        assertEquals( 1, result.errors().size() );
        assertEquals( 2, result.errors().get( 0 ).recordNumber() );
        assertTrue( result.errors().get( 0 ).error().message().contains( "DECIMAL(3, 1)" ) );
        assertThrows( MappingValidationException.class,
                () -> state.previewColumn( new ColumnName( column.id, column.name, true, PolyType.DECIMAL, false, 2, 3, "CONVERT_COLUMN" ) ) );
    }


    @Test
    void acceptsNullSourceValuesRegardlessOfTargetNullability() {
        var state = state();
        var sourceType = column.getAlgDataType( factory );
        for ( boolean nullable : List.of( true, false ) ) {
            var target = state.previewColumn( new ColumnName( column.id, column.name, true, PolyType.INTEGER, nullable, null, null, "CONVERT_COLUMN" ) );
            var result = MappingConversionValidator.checkColumn( state, target, sourceType, 0,
                    List.of( Collections.singletonList( null ), List.of( PolyString.of( "42" ) ) ), context );
            assertEquals( 2, result.checkedRecords() );
            assertTrue( result.errors().isEmpty() );
            assertEquals( nullable, target.nullable );
        }
    }


    @Test
    void acceptsShortAndEmptySamplesAndNullableValues() {
        var state = state();
        var target = state.previewColumn( new ColumnName( column.id, column.name, true, PolyType.INTEGER, true, null, null, "CONVERT_COLUMN" ) );
        var sourceType = column.getAlgDataType( factory );
        var shortSample = MappingConversionValidator.checkColumn( state, target, sourceType, 0,
                List.of( List.of( PolyString.of( "42" ) ), Collections.singletonList( null ) ), context );
        assertEquals( 2, shortSample.checkedRecords() );
        assertTrue( shortSample.errors().isEmpty() );
        var emptySample = MappingConversionValidator.checkColumn( state, target, sourceType, 0, List.of(), context );
        assertEquals( 0, emptySample.checkedRecords() );
        assertTrue( emptySample.errors().isEmpty() );
    }

}

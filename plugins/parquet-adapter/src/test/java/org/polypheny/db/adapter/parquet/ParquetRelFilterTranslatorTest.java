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

package org.polypheny.db.adapter.parquet;

import java.util.List;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.shared.filter.FilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNativeFilterBuilder;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.nodes.SpecialOperator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetRelFilterTranslatorTest {

    private AlgDataType intType;
    private AlgDataType boolType;
    private ParquetRelFilterTranslator translator;


    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @BeforeEach
    void setUp() {
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        intType = typeFactory.createPolyType( PolyType.INTEGER );
        boolType = typeFactory.createPolyType( PolyType.BOOLEAN );
        translator = new ParquetRelFilterTranslator();
    }


    @Test
    void translatesAndOrTree() {
        RexNode filter = call(
                Kind.AND,
                call( Kind.GREATER_THAN, ref( 0 ), param( 10 ) ),
                call(
                        Kind.OR,
                        call( Kind.EQUALS, ref( 1 ), param( 20 ) ),
                        call( Kind.EQUALS, ref( 1 ), param( 30 ) ) ) );

        ParquetAdapterFilter translated = translator.translate( List.of( PolyType.INTEGER, PolyType.INTEGER ), filter );

        assertNotNull( translated );
        assertEquals( Kind.AND, translated.operator() );
        assertEquals( 2, translated.operands().size() );
        assertEquals( Kind.GREATER_THAN, translated.operands().get( 0 ).operator() );
        assertEquals( Kind.OR, translated.operands().get( 1 ).operator() );
        assertEquals( 2, translated.operands().get( 1 ).operands().size() );
    }


    @Test
    void translatesInAsOrOfEquals() {
        RexNode filter = call( Kind.IN, ref( 0 ), param( 10 ), param( 20 ), param( 30 ) );

        ParquetAdapterFilter translated = translator.translate( List.of( PolyType.INTEGER ), filter );

        assertNotNull( translated );
        assertEquals( Kind.OR, translated.operator() );
        assertEquals( 3, translated.operands().size() );
        assertTrue( translated.operands().stream().allMatch( operand -> operand.operator() == Kind.EQUALS && operand.columnIndex() == 0 ) );
    }


    @Test
    void translatesNullChecks() {
        ParquetAdapterFilter isNull = translator.translate( List.of( PolyType.INTEGER ), call( Kind.IS_NULL, ref( 0 ) ) );
        ParquetAdapterFilter isNotNull = translator.translate( List.of( PolyType.INTEGER ), call( Kind.IS_NOT_NULL, ref( 0 ) ) );

        assertNotNull( isNull );
        assertEquals( Kind.IS_NULL, isNull.operator() );
        assertEquals( 0, isNull.columnIndex() );
        assertNull( isNull.polyValue() );

        assertNotNull( isNotNull );
        assertEquals( Kind.IS_NOT_NULL, isNotNull.operator() );
        assertEquals( 0, isNotNull.columnIndex() );
        assertNull( isNotNull.polyValue() );
    }


    @Test
    void evaluatesNullChecks() {
        TestFilterEvaluator evaluator = new TestFilterEvaluator();

        assertEquals( Boolean.TRUE, evaluator.evaluate( PolyNull.NULL, new ParquetAdapterFilter( 0, Kind.IS_NULL, null ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( PolyString.of( "shipped" ), new ParquetAdapterFilter( 0, Kind.IS_NULL, null ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( PolyNull.NULL, new ParquetAdapterFilter( 0, Kind.IS_NOT_NULL, null ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( PolyString.of( "shipped" ), new ParquetAdapterFilter( 0, Kind.IS_NOT_NULL, null ) ) );
    }


    @Test
    void reversesComparisonWhenLiteralIsOnTheLeft() {
        RexNode filter = call( Kind.LESS_THAN, param( 10 ), ref( 0 ) );

        ParquetAdapterFilter translated = translator.translate( List.of( PolyType.INTEGER ), filter );

        assertNotNull( translated );
        assertEquals( Kind.GREATER_THAN, translated.operator() );
    }


    @Test
    void ignoresUnsupportedInt96TimestampPredicatePushdown() {
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.INT96 )
                .named( "ts" )
                .named( "test_schema" );

        ParquetAdapterFilter filter = new ParquetAdapterFilter( 0, Kind.EQUALS, PolyTimestamp.of( 1_700_000_000_000L ) );

        assertDoesNotThrow(
                () -> ParquetNativeFilterBuilder.build( schema, List.of( filter ) ) );
    }


    @Test
    void buildsNativeNullCheckPredicates() {
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.BINARY )
                .named( "status" )
                .named( "test_schema" );

        assertDoesNotThrow(
                () -> ParquetNativeFilterBuilder.build( schema, List.of( new ParquetAdapterFilter( 0, Kind.IS_NOT_NULL, null ) ) ) );
    }


    private RexNode ref( int index ) {
        return new RexIndexRef( index, intType );
    }


    private RexNode param( int index ) {
        return new RexDynamicParam( intType, index );
    }


    private RexNode call( Kind kind, RexNode... operands ) {
        return new RexCall( boolType, new SpecialOperator( kind.name(), kind ), operands );
    }


    private static class TestFilterEvaluator extends FilterEvaluator<PolyValue> {

        @Override
        protected Boolean evaluateLeaf( PolyValue value, ParquetAdapterFilter filter ) {
            return matchesValue( value, filter.operator(), filter.polyValue() );
        }

    }
}

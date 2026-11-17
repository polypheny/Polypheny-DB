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

package org.polypheny.db.algebra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.SpecialOperator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PlanKeyTest {

    private final AlgDataTypeFactory types = AlgDataTypeFactory.DEFAULT;
    private final RexBuilder rex = new RexBuilder( types );


    @BeforeAll
    static void initializeTestHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void javaHashCollisionsDoNotMergeLiteralKeys() {
        assertEquals( "Aa".hashCode(), "BB".hashCode() );
        Map<PlanKey, String> entries = new HashMap<>();
        entries.put( key( rex.makeLiteral( "Aa" ) ), "first" );
        entries.put( key( rex.makeLiteral( "BB" ) ), "second" );

        assertEquals( 2, entries.size() );
        assertEquals( "first", entries.get( key( rex.makeLiteral( "Aa" ) ) ) );
        assertEquals( "second", entries.get( key( rex.makeLiteral( "BB" ) ) ) );
    }


    @Test
    void dynamicParameterKeysUseIndexAndFullType() {
        AlgDataType text = types.createPolyType( PolyType.VARCHAR, 20 );
        PlanKey parameter = key( rex.makeDynamicParam( text, 0 ) );
        assertEquals( parameter, key( rex.makeDynamicParam( types.createPolyType( PolyType.VARCHAR, 20 ), 0 ) ) );
        assertNotEquals( parameter, key( rex.makeDynamicParam( text, 1 ) ) );
        assertNotEquals( parameter, key( rex.makeDynamicParam( types.createPolyType( PolyType.VARCHAR, 40 ), 0 ) ) );
        assertNotEquals( parameter, key( rex.makeDynamicParam( types.createTypeWithNullability( text, true ), 0 ) ) );
    }


    @Test
    void orderedOperandsAndTypedValuesRemainDistinct() {
        assertNotEquals( key( List.of( "a, b", "c" ) ), key( List.of( "a", "b, c" ) ) );
        assertNotEquals( key( List.of( "Aa", "BB" ) ), key( List.of( "BB", "Aa" ) ) );
        assertNotEquals( key( PolyBigDecimal.of( "1.00" ) ), key( PolyBigDecimal.of( "1.0" ) ) );
        assertNotEquals( key( PolyFloat.of( 1f ) ), key( PolyDouble.of( 1d ) ) );
        assertNotEquals( key( PolyDouble.of( -0d ) ), key( PolyDouble.of( 0d ) ) );
    }


    @Test
    void expressionKeysIncludeRegisteredOperatorIdentityAndOperandOrder() {
        QueryLanguage firstLanguage = testLanguage( "plan-key-first" );
        QueryLanguage secondLanguage = testLanguage( "plan-key-second" );
        SpecialOperator first = new SpecialOperator( "+", Kind.PLUS );
        SpecialOperator second = new SpecialOperator( "+", Kind.PLUS );
        OperatorRegistry.register( firstLanguage, OperatorName.PLUS, first );
        OperatorRegistry.register( secondLanguage, OperatorName.PLUS, second );
        try {
            AlgDataType resultType = types.createPolyType( PolyType.INTEGER );
            RexNode left = rex.makeDynamicParam( resultType, 0 );
            RexNode right = rex.makeDynamicParam( resultType, 1 );
            PlanKey key = key( rex.makeCall( resultType, first, left, right ) );

            assertEquals( key, key( rex.makeCall( resultType, first, left, right ) ) );
            assertNotEquals( key, key( rex.makeCall( resultType, second, left, right ) ) );
            assertNotEquals( key, key( rex.makeCall( resultType, first, right, left ) ) );
            assertTrue( PlanKey.of( rex.makeCall( resultType, new SpecialOperator( "+", Kind.PLUS ), left, right ) ).isEmpty() );
        } finally {
            OperatorRegistry.remove( firstLanguage );
            OperatorRegistry.remove( secondLanguage );
        }
    }


    @Test
    void binaryKeysSnapshotBytesAndBitCount() {
        byte[] bytes = { 0x08 };
        PlanKey original = key( PolyBinary.of( bytes, 5 ) );
        bytes[0] = 0x03;

        assertEquals( original, key( PolyBinary.of( new byte[]{ 0x08 }, 5 ) ) );
        assertNotEquals( original, key( PolyBinary.of( bytes, 5 ) ) );
        assertNotEquals( original, key( PolyBinary.of( new byte[]{ 0x08 }, 4 ) ) );
    }


    @Test
    void arbitraryComponentsBypassStructuralKeyCreation() {
        assertTrue( PlanKey.of( List.of( "supported", new Object() ) ).isEmpty() );
    }


    private static QueryLanguage testLanguage( String name ) {
        return new QueryLanguage( DataModel.RELATIONAL, name, List.of(), null, () -> null, null, context -> List.of(), Function.identity() );
    }


    private static PlanKey key( Object component ) {
        return PlanKey.of( component ).orElseThrow();
    }

}

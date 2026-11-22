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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
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
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.SpecialOperator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.snapshot.MockSnapshot;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.VectorType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolySymbol;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.GraphObject;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;
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
    void nameReferenceKeysPreserveThePathUsedByNativeAdapters() {
        AlgDataType type = types.createPolyType( PolyType.INTEGER );
        RexNameRef direct = RexNameRef.create( "a.b", null, type );
        RexNameRef withEmptySegment = RexNameRef.create( "a..b", null, type );

        assertEquals( direct.names, withEmptySegment.names );
        assertNotEquals( key( direct ), key( withEmptySegment ) );
    }


    @Test
    void vectorKeysSnapshotTheInheritedArrayDimension() {
        AlgDataType elementType = types.createPolyType( PolyType.FLOAT );
        VectorType vector = new VectorType( elementType, false, 8, VectorType.ElementType.FLOAT );
        PlanKey original = key( vector );

        vector.setDimension( 2 );

        assertEquals( original, key( new VectorType( elementType, false, 8, VectorType.ElementType.FLOAT ) ) );
        assertNotEquals( original, key( vector ) );
    }


    @Test
    void symbolKeysDistinguishObjectValuesAndRejectUnsupportedValues() {
        assertEquals( key( PolySymbol.of( "a" ) ), key( PolySymbol.of( "a" ) ) );
        assertNotEquals( key( PolySymbol.of( "a" ) ), key( PolySymbol.of( "b" ) ) );
        assertTrue( PlanKey.of( PolySymbol.of( new Object() ) ).isEmpty() );
    }


    @Test
    void differentCatalogSnapshotsDoNotShareKeysWhenAnIdIsReused() {
        Snapshot original = new ReusedIdSnapshot();
        Snapshot replacement = new ReusedIdSnapshot();

        assertEquals( original.id(), replacement.id() );
        assertEquals( key( original ), key( original ) );
        assertNotEquals( key( original ), key( replacement ) );
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
    void cypherNodeMatcherKeysIgnoreOnlyThePatternIdentity() {
        QueryLanguage cypher = testLanguage( DataModel.GRAPH, "cypher" );
        QueryLanguage other = testLanguage( DataModel.GRAPH, "plan-key-other-graph" );
        Operator match = new LangFunctionOperator( "nodeMatch", Kind.CYPHER_FUNCTION, PolyType.NODE );
        Operator otherMatch = new LangFunctionOperator( "nodeMatch", Kind.CYPHER_FUNCTION, PolyType.NODE );
        OperatorRegistry.register( cypher, OperatorName.CYPHER_NODE_MATCH, match );
        OperatorRegistry.register( other, OperatorName.CYPHER_NODE_MATCH, otherMatch );
        try {
            AlgDataType type = types.createPolyType( PolyType.NODE );
            PolyNode first = node( "n", "Person", 1 );
            PolyNode equivalent = node( "n", "Person", 1 );
            assertNotEquals( first.id, equivalent.id );
            assertEquals( matchKey( match, type, first ), matchKey( match, type, equivalent ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, node( "n", "Place", 1 ) ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, node( "n", "Person", 2 ) ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, node( "other", "Person", 1 ) ) );
            assertNotEquals( matchKey( otherMatch, type, first ), matchKey( otherMatch, type, equivalent ) );
            assertNotEquals( key( new RexLiteral( first, type, PolyType.NODE ) ), key( new RexLiteral( equivalent, type, PolyType.NODE ) ) );
        } finally {
            OperatorRegistry.remove( cypher );
            OperatorRegistry.remove( other );
        }
    }


    @Test
    void cypherPathMatcherKeysPreserveRelationshipSemanticsAndLocalReferences() {
        QueryLanguage cypher = testLanguage( DataModel.GRAPH, "cypher" );
        Operator match = new LangFunctionOperator( "pathMatch", Kind.CYPHER_FUNCTION, PolyType.PATH );
        OperatorRegistry.register( cypher, OperatorName.CYPHER_PATH_MATCH, match );
        try {
            PolyNode left = node( "a", "Person", 1 );
            PolyNode right = node( "b", "Person", 2 );
            PolyPath first = path( left, right, "KNOWS", EdgeDirection.LEFT_TO_RIGHT, 2 );
            PolyPath equivalent = path( node( "a", "Person", 1 ), node( "b", "Person", 2 ), "KNOWS", EdgeDirection.LEFT_TO_RIGHT, 2 );
            AlgDataType type = types.createPathType( first.getPathType( types ) );

            assertNotEquals( first.id, equivalent.id );
            assertEquals( matchKey( match, type, first ), matchKey( match, type, equivalent ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, path( left, right, "LIKES", EdgeDirection.LEFT_TO_RIGHT, 2 ) ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, path( left, right, "KNOWS", EdgeDirection.RIGHT_TO_LEFT, 2 ) ) );
            assertNotEquals( matchKey( match, type, first ), matchKey( match, type, path( left, right, "KNOWS", EdgeDirection.LEFT_TO_RIGHT, 3 ) ) );
            assertNotEquals( matchKey( match, type, path( left, left, "KNOWS", EdgeDirection.LEFT_TO_RIGHT, 2 ) ),
                    matchKey( match, type, path( left, node( "a", "Person", 1 ), "KNOWS", EdgeDirection.LEFT_TO_RIGHT, 2 ) ) );
            assertNotEquals( key( new RexLiteral( first, type, PolyType.PATH ) ), key( new RexLiteral( equivalent, type, PolyType.PATH ) ) );

            PolyPath unbounded = path( left, right, "KNOWS", EdgeDirection.LEFT_TO_RIGHT, null );
            PlanKey unboundedKey = matchKey( match, type, unbounded );
            assertNotEquals( matchKey( match, type, first ), unboundedKey );
            assertEquals( unboundedKey, matchKey( match, type, path( node( "a", "Person", 1 ), node( "b", "Person", 2 ), "KNOWS", EdgeDirection.LEFT_TO_RIGHT, null ) ) );
            unbounded.getEdges().get( 0 ).fromTo( Pair.of( 2, null ) );
            assertNotEquals( unboundedKey, matchKey( match, type, unbounded ) );
        } finally {
            OperatorRegistry.remove( cypher );
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


    @Test
    void cyclicComponentsBypassStructuralKeyCreation() {
        List<Object> list = new ArrayList<>();
        list.add( List.of( list ) );
        assertTrue( PlanKey.of( list ).isEmpty() );

        PolyDocument document = new PolyDocument( new HashMap<>() );
        document.put( PolyString.of( "self" ), document );
        assertTrue( PlanKey.of( document ).isEmpty() );
    }


    @Test
    void sharingAcyclicComponentsDoesNotChangeTheirKeys() {
        List<String> shared = new ArrayList<>( List.of( "value" ) );
        assertEquals( key( List.of( shared, shared ) ), key( List.of( List.of( "value" ), List.of( "value" ) ) ) );
    }


    @Test
    void deeplyNestedComponentsBypassStructuralKeyCreation() {
        Object nested = "value";
        for ( int i = 0; i < 4096; i++ ) {
            nested = List.of( nested );
        }

        assertTrue( PlanKey.of( nested ).isEmpty() );
    }


    @Test
    void sharedValueAndPlanKeysCompareWithoutExpandingTheSameSubtreeRepeatedly() {
        assertTimeoutPreemptively( Duration.ofSeconds( 5 ), () -> {
            PlanKey original = sharedKey( "Aa" );
            PlanKey collision = sharedKey( "BB" );

            assertEquals( original, sharedKey( "Aa" ) );
            assertEquals( original.hashCode(), collision.hashCode() );
            assertNotEquals( original, collision );
        } );
    }


    private static PlanKey sharedKey( String literal ) {
        Object compound = literal;
        for ( int depth = 0; depth < 48; depth++ ) {
            compound = List.of( compound, compound );
        }
        PlanKey plan = key( compound );
        for ( int depth = 0; depth < 48; depth++ ) {
            plan = key( List.of( plan, plan ) );
        }
        return plan;
    }


    private static QueryLanguage testLanguage( String name ) {
        return testLanguage( DataModel.RELATIONAL, name );
    }


    private static QueryLanguage testLanguage( DataModel model, String name ) {
        return new QueryLanguage( model, name, List.of(), null, () -> null, null, context -> List.of(), Function.identity() );
    }


    private static PolyNode node( String variable, String label, int value ) {
        return new PolyNode( new PolyDictionary( Map.of( PolyString.of( "value" ), PolyInteger.of( value ) ) ),
                List.of( PolyString.of( label ) ), PolyString.of( variable ) );
    }


    private static PolyPath path( PolyNode left, PolyNode right, String label, EdgeDirection direction, Integer maxLength ) {
        PolyEdge edge = new PolyEdge( new PolyDictionary(), List.of( PolyString.of( label ) ), left.id, right.id, direction, PolyString.of( "r" ) );
        edge.fromTo( Pair.of( 1, maxLength ) );
        return PolyPath.create( List.of( Pair.of( left.variableName, left ), Pair.of( right.variableName, right ) ), List.of( Pair.of( edge.variableName, edge ) ) );
    }


    private PlanKey matchKey( Operator match, AlgDataType type, GraphObject pattern ) {
        return key( rex.makeCall( type, match, rex.makeInputRef( GraphType.of(), 0 ), new RexLiteral( pattern, type, pattern.getType() ) ) );
    }


    private static PlanKey key( Object component ) {
        return PlanKey.of( component ).orElseThrow();
    }


    private static final class ReusedIdSnapshot extends MockSnapshot {

        @Override
        public long id() {
            return 1;
        }

    }

}

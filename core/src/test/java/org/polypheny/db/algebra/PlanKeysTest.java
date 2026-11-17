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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgFieldCollation.NullDirection;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.snapshot.MockSnapshot;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class PlanKeysTest {

    @BeforeAll
    static void initializeTestHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void graphProjectionsDistinguishBoundLiteralsAndAliases() {
        AlgCluster cluster = cluster( ModelTrait.GRAPH );
        PlanKey original = key( graphProjection( cluster, "Aa", "name" ) );

        assertEquals( original, key( graphProjection( cluster( ModelTrait.GRAPH ), "Aa", "name" ) ) );
        assertNotEquals( original, key( graphProjection( cluster, "BB", "name" ) ) );
        assertNotEquals( original, key( graphProjection( cluster, "Aa", "alias" ) ) );
    }


    @Test
    void documentProjectionsDistinguishAddedExpressions() {
        AlgCluster cluster = cluster( ModelTrait.DOCUMENT );
        PlanKey original = key( documentProjection( cluster, "Aa" ) );

        assertEquals( original, key( documentProjection( cluster( ModelTrait.DOCUMENT ), "Aa" ) ) );
        assertNotEquals( original, key( documentProjection( cluster, "BB" ) ) );
    }


    @Test
    void relationalProjectionsReuseEquivalentParameterSlots() {
        AlgCluster cluster = cluster( ModelTrait.RELATIONAL );
        PlanKey original = key( parameterProjection( cluster, 0 ) );

        assertEquals( original, key( parameterProjection( cluster( ModelTrait.RELATIONAL ), 0 ) ) );
        assertNotEquals( original, key( parameterProjection( cluster, 1 ) ) );
    }


    @Test
    void relationalKeysDistinguishSortDirectionAndNullPlacement() {
        AlgNode values = parameterProjection( cluster( ModelTrait.RELATIONAL ), 0 );
        AlgCollation ascendingNullsLast = AlgCollations.of( new AlgFieldCollation( 0, Direction.ASCENDING, NullDirection.LAST ) );
        AlgCollation ascendingNullsFirst = AlgCollations.of( new AlgFieldCollation( 0, Direction.ASCENDING, NullDirection.FIRST ) );
        AlgCollation descendingNullsLast = AlgCollations.of( new AlgFieldCollation( 0, Direction.DESCENDING, NullDirection.LAST ) );
        PlanKey original = key( LogicalRelSort.create( values, ascendingNullsLast, null, null ) );

        assertNotEquals( original, key( LogicalRelSort.create( values, ascendingNullsFirst, null, null ) ) );
        assertNotEquals( original, key( LogicalRelSort.create( values, descendingNullsLast, null, null ) ) );
    }


    @Test
    void unsupportedNodeSubclassMakesTheWholeTreeUncacheable() {
        AlgCluster cluster = cluster( ModelTrait.GRAPH );
        List<RexNode> projects = List.of( cluster.getRexBuilder().makeLiteral( "Aa" ) );
        List<PolyString> names = List.of( PolyString.of( "name" ) );
        AlgNode unsupported = new LogicalLpgProject( cluster, cluster.traitSet(), graphValues( cluster ), projects, names ) {};

        assertTrue( PlanKeys.forTree( unsupported ).isEmpty() );
        assertTrue( PlanKeys.forTree( LogicalLpgProject.create( unsupported, projects, names ) ).isEmpty() );
    }


    private static AlgNode graphProjection( AlgCluster cluster, String value, String name ) {
        return LogicalLpgProject.create( graphValues( cluster ), List.of( cluster.getRexBuilder().makeLiteral( value ) ), List.of( PolyString.of( name ) ) );
    }


    private static AlgNode graphValues( AlgCluster cluster ) {
        return LogicalLpgValues.create( cluster, cluster.traitSet(), cluster.getTypeFactory().builder().build(), ImmutableList.of( ImmutableList.of() ) );
    }


    private static AlgNode documentProjection( AlgCluster cluster, String value ) {
        AlgNode input = LogicalDocumentValues.create( cluster, List.of( PolyDocument.EMPTY_DOCUMENT ) );
        return LogicalDocumentProject.create( input, Map.of(), List.of(), Map.of( "added", cluster.getRexBuilder().makeLiteral( value ) ) );
    }


    private static AlgNode parameterProjection( AlgCluster cluster, int index ) {
        AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.INTEGER );
        return LogicalRelProject.create( LogicalRelValues.createOneRow( cluster ), List.of( cluster.getRexBuilder().makeDynamicParam( type, index ) ), List.of( "supplied" ) );
    }


    private static AlgCluster cluster( ModelTrait model ) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
        return AlgCluster.create( planner, new RexBuilder( AlgDataTypeFactory.DEFAULT ), planner.emptyTraitSet().replace( model ), new MockSnapshot() );
    }


    private static PlanKey key( AlgNode node ) {
        return PlanKeys.forTree( node ).orElseThrow();
    }

}

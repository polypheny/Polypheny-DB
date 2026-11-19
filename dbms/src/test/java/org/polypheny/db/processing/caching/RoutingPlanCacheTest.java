/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.processing.caching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.PlanKey;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.ResultKind;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.snapshot.MockSnapshot;

@Tag("adapter")
class RoutingPlanCacheTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void cacheKeepsDifferentConstraintChecksWithTheSameHashApart() {
        Snapshot snapshot = new MockSnapshot();
        AlgRoot firstRoot = constraintCheck( snapshot, "Aa" );
        AlgRoot secondRoot = constraintCheck( snapshot, "BB" );
        PlanKey first = key( firstRoot, Map.of(), snapshot );
        PlanKey second = key( secondRoot, Map.of(), snapshot );
        PlanKey equivalentFirst = key( constraintCheck( snapshot, "Aa" ), Map.of(), snapshot );
        assertEquals( first.hashCode(), second.hashCode() );
        assertNotEquals( first, second );

        RoutingPlanCache cache = new RoutingPlanCache();
        CachedProposedRoutingPlan firstPlan = new CachedProposedRoutingPlan( new AllocationTable( 1, 2, 3, 4, 5, 6 ), "first" );
        CachedProposedRoutingPlan secondPlan = new CachedProposedRoutingPlan( new AllocationTable( 8, 9, 10, 11, 12, 13 ), "second" );
        cache.put( first, firstPlan );
        assertNull( cache.getIfPresent( second ) );
        cache.put( second, secondPlan );
        assertEquals( firstPlan, cache.getIfPresent( equivalentFirst ) );
        assertEquals( secondPlan, cache.getIfPresent( second ) );
    }


    @Test
    void routingKeySnapshotsPartitionListsByEntity() {
        Snapshot snapshot = new MockSnapshot();
        AlgRoot root = constraintCheck( snapshot, "Aa" );
        List<Long> mutablePartitions = new ArrayList<>( List.of( 10L, 11L ) );
        Map<Long, List<Long>> partitions = new LinkedHashMap<>();
        partitions.put( 1L, mutablePartitions );
        partitions.put( 2L, List.of( 12L ) );
        PlanKey first = key( root, partitions, snapshot );
        mutablePartitions.clear();

        Map<Long, List<Long>> equivalent = new LinkedHashMap<>();
        equivalent.put( 2L, List.of( 12L ) );
        equivalent.put( 1L, List.of( 10L, 11L ) );
        assertEquals( first, key( root, equivalent, snapshot ) );
        assertNotEquals( first, key( root, Map.of( 1L, List.of( 12L ), 2L, List.of( 10L, 11L ) ), snapshot ) );
        assertNotEquals( first, key( root, Map.of( 1L, List.of( 11L, 10L ), 2L, List.of( 12L ) ), snapshot ) );
    }


    @Test
    void catalogSnapshotChangesPreventRoutingPlanReuse() {
        Snapshot first = new MockSnapshot();
        Snapshot next = new MockSnapshot();
        AlgRoot firstRoot = constraintCheck( first, "Aa" );
        assertTrue( RoutingPlanCache.keyFor( firstRoot, Map.of(), next ).isEmpty() );
        assertNotEquals( key( firstRoot, Map.of(), first ), key( constraintCheck( next, "Aa" ), Map.of(), next ) );
    }


    @Test
    void routingKeyDistinguishesDeclaredResultKindsForTheSamePlan() {
        Snapshot snapshot = new MockSnapshot();
        AlgRoot updateCount = constraintCheck( snapshot, "Aa" );
        AlgRoot returnedRows = updateCount.withResultKind( ResultKind.ROWS );

        assertNotEquals( key( updateCount, Map.of(), snapshot ), key( returnedRows, Map.of(), snapshot ) );
    }


    private static PlanKey key( AlgRoot root, Map<Long, List<Long>> partitions, Snapshot snapshot ) {
        return RoutingPlanCache.keyFor( root, partitions, snapshot ).orElseThrow();
    }


    private static AlgRoot constraintCheck( Snapshot snapshot, String value ) {
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
        planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
        RexBuilder rex = new RexBuilder( AlgDataTypeFactory.DEFAULT );
        AlgCluster cluster = AlgCluster.create( planner, rex, planner.emptyTraitSet().replace( ModelTrait.RELATIONAL ), snapshot );
        AlgNode input = LogicalRelValues.createOneRow( cluster );
        AlgNode control = LogicalRelProject.create( input, List.of( rex.makeLiteral( value ) ), List.of( "checked" ) );
        AlgNode enforcer = LogicalConstraintEnforcer.create( input, control, List.of( IllegalStateException.class ), List.of( "Constraint violation" ) );
        return AlgRoot.of( enforcer, Kind.INSERT );
    }

}

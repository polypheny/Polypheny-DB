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

package org.polypheny.db.monitoring.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.PlanKeys;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.routing.LogicalQueryInformation;

class PlanFingerprintTest {

    @Test
    void equivalentPlanShapesHaveTheSameFingerprint() {
        AlgNode scan = node( LogicalRelScan.class );
        AlgNode sharedInputs = node( LogicalRelJoin.class, scan, scan );
        AlgNode independentInputs = node( LogicalRelJoin.class, node( LogicalRelScan.class ), node( LogicalRelScan.class ) );

        assertEquals( PlanFingerprint.of( sharedInputs ), PlanFingerprint.of( independentInputs ) );
    }


    @Test
    void fingerprintsDistinguishTheOperatorAndTheOrderOfItsInputs() {
        AlgNode scan = node( LogicalRelScan.class );
        AlgNode values = node( LogicalRelValues.class );
        String original = PlanFingerprint.of( node( LogicalRelJoin.class, scan, values ) );

        assertNotEquals( original, PlanFingerprint.of( node( LogicalRelJoin.class, values, scan ) ) );
        assertNotEquals( original, PlanFingerprint.of( node( LogicalRelUnion.class, scan, values ) ) );
    }


    @Test
    void fingerprintRetainsTheVersionOneEncoding() {
        AlgCluster cluster = mock( AlgCluster.class );
        AlgNode leaf = new ShapeNode( cluster, List.of() );
        AlgNode left = new ShapeNode( cluster, List.of( leaf ) );
        AlgNode root = new ShapeNode( cluster, List.of( leaf, left ) );

        assertEquals( "shape-v1:7549871a97f8fb8825f5b2bda61e8196c7b177bd2cdd3c5e669a35ac864623ac", PlanFingerprint.of( root ) );
    }


    @Test
    void fingerprintsDeepPlansWithSharedInputs() {
        AlgCluster cluster = mock( AlgCluster.class );
        AlgNode leaf = new ShapeNode( cluster, List.of() );
        AlgNode plan = leaf;
        for ( int level = 0; level < 8192; level++ ) {
            plan = new ShapeNode( cluster, List.of( plan, leaf ) );
        }

        assertTrue( PlanFingerprint.of( plan ).matches( "shape-v1:[0-9a-f]{64}" ) );
    }


    @Test
    void omitsFingerprintsForCyclicOrIncompletePlans() {
        AlgNode cyclic = mock( AlgNode.class );
        when( cyclic.getInputs() ).thenReturn( List.of( cyclic ) );
        AlgCluster cluster = mock( AlgCluster.class );
        AlgNode missingInputs = new ShapeNode( cluster, null );
        AlgNode missingChild = new ShapeNode( cluster, Collections.singletonList( null ) );

        assertNull( PlanFingerprint.of( cyclic ) );
        assertNull( PlanFingerprint.of( missingInputs ) );
        assertNull( PlanFingerprint.of( missingChild ) );
    }


    @Test
    void monitoringRetainsTheFingerprintOfAnUncacheablePlan() {
        AlgNode plan = node( EnumerableLimit.class, node( LogicalRelScan.class ) );
        assertTrue( PlanKeys.forTree( plan ).isEmpty() );

        String fingerprint = PlanFingerprint.of( plan );
        assertTrue( fingerprint.matches( "shape-v1:[0-9a-f]{64}" ) );

        QueryEvent event = new QueryEvent();
        event.setLogicalQueryInformation( mock( LogicalQueryInformation.class ) );
        event.setPlanFingerprint( fingerprint );

        QueryDataPointImpl point = (QueryDataPointImpl) event.analyze().get( 0 );
        assertEquals( fingerprint, point.getPlanFingerprint() );
    }


    private static AlgNode node( Class<? extends AlgNode> operator, AlgNode... inputs ) {
        AlgNode node = mock( operator );
        when( node.getInputs() ).thenReturn( List.of( inputs ) );
        return node;
    }


    private static final class ShapeNode extends AbstractAlgNode {

        private final List<AlgNode> inputs;


        private ShapeNode( AlgCluster cluster, List<AlgNode> inputs ) {
            super( cluster, AlgTraitSet.createEmpty() );
            this.inputs = inputs;
        }


        @Override
        public List<AlgNode> getInputs() {
            return inputs;
        }

    }

}

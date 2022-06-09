/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.algebra.rules;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.graph.LogicalGraphValues;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;

public class GraphToRelRule extends AlgOptRule {

    final boolean isModify;

    public static final GraphToRelRule GRAPH_MODIFY_TO_REL =
            new GraphToRelRule(
                    true,
                    operand( LogicalGraphModify.class, operand( LogicalGraphValues.class, none() ) ),
                    "GRAPH_MODIFY_TO_REL" );

    public static final GraphToRelRule GRAPH_SCAN_TO_REL =
            new GraphToRelRule(
                    false,
                    operand( LogicalGraphScan.class, none() ),
                    "GRAPH_SCAN_TO_REL" );


    public GraphToRelRule( boolean isModify, AlgOptRuleOperand operand, String description ) {
        super( operand, AlgFactories.LOGICAL_BUILDER, description );
        this.isModify = isModify;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {

        AlgNode res;
        if ( isModify ) {
            //res = getRelModify( call );
        } else {
            //res = getRelScan( call );
        }
        //call.transformTo( res );
    }


    /*
    private AlgNode getRelModify( AlgOptRuleCall call ) {
        AlgNode node = getRelModifyUntransformed( call );

        return LogicalTransformer.create( List.of( node ), node.getTraitSet(), ModelTrait.RELATIONAL, ModelTrait.GRAPH, node.getRowType() );
    }*/


    /*private AlgNode getRelModifyUntransformed( AlgOptRuleCall call ) {
        LogicalGraphModify modify = call.alg( 0 );
        LogicalGraphValues values = call.alg( 1 );

        List<AlgNode> transformedValues = values.getRelationalEquivalent( List.of(), Arrays.asList( modify.getNodeTable(), modify.getNodePropertyTable(), modify.getEdgeTable(), modify.getEdgePropertyTable() ) );

        List<AlgNode> transformedModifies = modify.getRelationalEquivalent( transformedValues, List.of() );

        AlgTraitSet set = modify.getTraitSet().replace( ModelTrait.RELATIONAL );

        if ( transformedModifies.size() == 1 ) {
            return transformedModifies.get( 0 );
        }

        return new LogicalModifyCollect( modify.getCluster(), set, transformedModifies, true );
    }*/


    /*private AlgNode getRelScan( AlgOptRuleCall call ) {
        LogicalGraphScan scan = call.alg( 0 );

        List<AlgOptTable> tables = new ArrayList<>();
        tables.add( scan.getNodeTable() );
        tables.add( scan.getNodePropertyTable() );
        if ( scan.getEdgeTable() != null ) {
            tables.add( scan.getEdgeTable() );
            tables.add( scan.getEdgePropertyTable() );
        }

        List<AlgNode> transformedScans = scan.getRelationalEquivalent( List.of(), tables );

        return LogicalTransformer.create( transformedScans, scan.getTraitSet().replace( ModelTrait.RELATIONAL ), ModelTrait.RELATIONAL, ModelTrait.GRAPH, scan.getRowType() );
    }*/

}

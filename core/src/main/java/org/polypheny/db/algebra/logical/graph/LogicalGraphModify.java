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

package org.polypheny.db.algebra.logical.graph;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;

public class LogicalGraphModify extends SingleAlg implements GraphAlg, RelationalTransformable {


    public final Operation operation;
    public final List<? extends RexNode> nodeOperations;
    public final List<? extends RexNode> edgeOperations;
    @Getter
    private final LogicalGraph graph;
    @Getter
    public final PolyphenyDbCatalogReader catalogReader;

    @Setter
    @Getter
    private AlgOptTable nodeTable;

    @Setter
    @Getter
    private AlgOptTable edgeTable;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param graph
     * @param catalogReader
     * @param input Input relational expression
     */
    public LogicalGraphModify( AlgOptCluster cluster, AlgTraitSet traits, LogicalGraph graph, PolyphenyDbCatalogReader catalogReader, AlgNode input, Operation operation, List<? extends RexNode> nodeOperations, List<? extends RexNode> edgeOperations ) {
        super( cluster, traits, input );
        assertLogicalGraphTrait( traits );
        this.operation = operation;
        this.nodeOperations = nodeOperations;
        this.edgeOperations = edgeOperations;
        this.graph = graph;
        // for the moment
        this.catalogReader = catalogReader;
        // for now
        this.rowType = AlgOptUtil.createDmlRowType( Kind.INSERT, getCluster().getTypeFactory() );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() +
                "$" + (nodeOperations != null ? nodeOperations.hashCode() : "[]") +
                "$" + (edgeOperations != null ? edgeOperations.hashCode() : "[]");
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        LogicalGraphModify modify = new LogicalGraphModify( inputs.get( 0 ).getCluster(), traitSet, graph, catalogReader, inputs.get( 0 ), operation, nodeOperations, edgeOperations );
        modify.setEdgeTable( edgeTable );
        modify.setNodeTable( nodeTable );
        return modify;
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, List<AlgOptTable> entities ) {
        PolyphenyDbCatalogReader catalogReader = this.catalogReader;

        AlgNode nodes = inputs.get( 0 );
        //AlgTraitSet out = input.getTraitSet().replace( ModelTrait.RELATIONAL );

        //modify of nodes

        Modify nodeModify = getModify( nodeTable, catalogReader, nodes );

        if ( inputs.size() == 1 ) {
            return List.of( nodeModify );
        }
        AlgNode edges = inputs.get( 1 );

        // modify of edges

        Modify edgeModify = getModify( edgeTable, catalogReader, edges );

        return Arrays.asList( nodeModify, edgeModify );
    }


    private Modify getModify( AlgOptTable table, PolyphenyDbCatalogReader catalogReader, AlgNode alg ) {
        return table.unwrap( ModifiableTable.class ).toModificationAlg( alg.getCluster(), table, catalogReader, alg, operation, null, null, true );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MODIFY;
    }

}

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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.LogicalModify;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;

public class LogicalGraphModify extends SingleAlg implements GraphAlg, RelationalTransformable {


    private final Operation operation;
    private final List<? extends RexNode> nodeOperations;
    private final List<? extends RexNode> relationshipOperations;
    private final LogicalGraph graph;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param graph
     * @param input Input relational expression
     */
    public LogicalGraphModify( AlgOptCluster cluster, AlgTraitSet traits, LogicalGraph graph, AlgNode input, Operation operation, List<? extends RexNode> nodeOperations, List<? extends RexNode> relationshipOperations ) {
        super( cluster, traits, input );
        assertLogicalGraphTrait( traits );
        this.operation = operation;
        this.nodeOperations = nodeOperations;
        this.relationshipOperations = relationshipOperations;
        this.graph = graph;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + nodeOperations.hashCode() + "$" + relationshipOperations.hashCode();
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, Statement statement ) {
        List<AlgNode> algNodes = ((RelationalTransformable) input).getRelationalEquivalent( List.of(), statement );
        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();

        AlgNode nodes = algNodes.get( 0 );
        //AlgTraitSet out = input.getTraitSet().replace( ModelTrait.RELATIONAL );

        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( graph.getNamespaceId() );
        //modify of nodes
        CatalogEntity nodeTable = Catalog.getInstance().getTable( mapping.nodeId );
        List<String> nodeNames = Arrays.asList( nodeTable.getNamespaceName(), nodeTable.name );

        LogicalModify nodeModify = new LogicalModify( nodes.getCluster(), nodes.getTraitSet(), catalogReader.getTable( nodeNames ), catalogReader, nodes, operation, null, null, true );

        if ( algNodes.size() == 1 ) {
            return List.of( nodeModify );
        }
        AlgNode edges = algNodes.get( 1 );

        // modify of edges
        CatalogEntity edgeTable = Catalog.getInstance().getTable( mapping.edgeId );
        List<String> edgeNames = Arrays.asList( edgeTable.getNamespaceName(), edgeTable.name );

        LogicalModify edgeModify = new LogicalModify( edges.getCluster(), edges.getTraitSet(), catalogReader.getTable( edgeNames ), catalogReader, edges, operation, null, null, true );

        return Arrays.asList( nodeModify, edgeModify );
    }

}

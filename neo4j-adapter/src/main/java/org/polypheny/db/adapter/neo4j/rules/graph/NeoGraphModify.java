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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.create_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.neo4j.NeoGraph;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.graph.GraphModify;
import org.polypheny.db.algebra.logical.graph.GraphProject;
import org.polypheny.db.algebra.logical.graph.GraphValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyNode;

public class NeoGraphModify extends GraphModify implements NeoGraphAlg {


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     * @param operation
     * @param ids
     * @param operations
     */
    public NeoGraphModify( AlgOptCluster cluster, AlgTraitSet traits, Graph graph, AlgNode input, Operation operation, List<String> ids, List<? extends RexNode> operations ) {
        super( cluster, traits, graph, input, operation, ids, operations, AlgOptUtil.createDmlRowType( Kind.INSERT, cluster.getTypeFactory() ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoGraphModify( inputs.get( 0 ).getCluster(), traitSet, graph, inputs.get( 0 ), operation, ids, operations );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.setGraph( (NeoGraph) getGraph() );
        implementor.setDml( true );
        implementor.visitChild( 0, getInput() );

        switch ( operation ) {

            case INSERT:
                handleInsert( implementor );
                break;
            case UPDATE:
                handleUpdate( implementor );
                break;
            case DELETE:
                handleDelete( implementor );
                break;
            case MERGE:
                throw new UnsupportedOperationException( "Merge is not supported by the graph implementation of the Neo4j adapter." );
        }
    }


    private void handleInsert( NeoGraphImplementor implementor ) {
        if ( implementor.getLast() instanceof GraphValues ) {
            GraphValues values = ((GraphValues) implementor.getLast());
            if ( values.getValues().isEmpty() ) {
                // node / edge insert
                implementor.statements.add( create_( list_( getCreatePath( values.getNodes(), values.getEdges(), implementor ) ) ) );
                return;
            } else {
                // normal values

            }
        } else {
            if ( !implementor.statements.isEmpty() ) {
                if ( implementor.getLast() instanceof GraphProject ) {
                    // match -> project -> create
                    GraphProject project = (GraphProject) implementor.getLast();
                    List<NeoStatement> statements = new ArrayList<>();
                    for ( RexNode projectProject : project.getProjects() ) {
                        Translator translator = new Translator( project.getRowType(), project.getInput().getRowType(), new HashMap<>(), null, implementor.getGraph().mappingLabel );
                        statements.add( literal_( projectProject.accept( translator ) ) );
                    }
                    implementor.add( create_( statements ) );
                    return;
                }
            }
        }
        throw new RuntimeException( "No values before modify." );
    }


    private List<RexNode> filterSpecialProjects( List<? extends RexNode> projects ) {
        List<Integer> filtered = new ArrayList<>();
        int i = 0;
        for ( RexNode project : projects ) {
            if ( (project instanceof RexCall) && ((RexCall) project).op.getOperatorName() == OperatorName.CYPHER_ADJUST_EDGE ) {
                filtered.add( i - 1 ); // extra node
                filtered.add( i ); // edge
                filtered.add( i + 1 ); // what to replace
            }
            i++;
        }
        List<RexNode> nodes = new ArrayList<>();
        int j = 0;
        for ( RexNode project : projects ) {
            if ( !filtered.contains( j ) ) {
                nodes.add( project );
            }
            j++;
        }
        return nodes;
    }


    private List<NeoStatement> getCreatePath( ImmutableList<PolyNode> nodes, ImmutableList<PolyEdge> edges, NeoGraphImplementor implementor ) {
        Map<String, String> uuidNameMapping = new HashMap<>();

        List<NeoStatement> statements = new ArrayList<>();

        int i = 0;
        for ( PolyNode node : nodes ) {
            String name = "n" + i;
            uuidNameMapping.put( node.id, name );
            statements.add( node_( name, node, implementor.getGraph().mappingLabel, true ) );
            i++;
        }
        for ( PolyEdge edge : edges ) {
            statements.add( path_( node_( uuidNameMapping.get( edge.source ) ), edge_( null, edge, true ), node_( uuidNameMapping.get( edge.target ) ) ) );
        }

        return statements;
    }


    private void handleUpdate( NeoGraphImplementor implementor ) {

    }


    private void handleDelete( NeoGraphImplementor implementor ) {

    }

}

/*
 * Copyright 2019-2024 The Polypheny Project
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
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.delete_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.set_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.neo4j.NeoGraph;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.LiteralStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

public class NeoLpgModify extends LpgModify<NeoGraph> implements NeoGraphAlg {


    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgModify}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link ModelTrait#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgModify( AlgCluster cluster, AlgTraitSet traits, NeoGraph graph, AlgNode input, Operation operation, List<PolyString> ids, List<? extends RexNode> operations ) {
        super( cluster, traits, graph, input, operation, ids, operations, AlgOptUtil.createDmlRowType( Kind.INSERT, cluster.getTypeFactory() ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgModify( inputs.get( 0 ).getCluster(), traitSet, entity, inputs.get( 0 ), operation, ids, operations );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.setGraph( entity );
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
        if ( implementor.getLast() instanceof LpgValues values ) {
            if ( values.getValues().isEmpty() ) {
                // node / edge insert
                implementor.statements.add( create_( list_( getCreatePath( values.getNodes(), values.getEdges(), implementor ) ) ) );
                return;
            } else {
                // normal values

            }
        } else {
            if ( !implementor.statements.isEmpty() ) {
                if ( implementor.getLast() instanceof LpgProject ) {
                    //List<NeoStatement> statements = buildReturnProject( implementor );
                    implementor.add( create_( buildReturnProject( (LpgProject) implementor.getLast(), implementor.getGraph().mappingLabel ) ) );
                    return;
                }
            }
        }
        throw new GenericRuntimeException( "No values before modify." );
    }


    public static List<NeoStatement> buildReturnProject( LpgProject project, String mappingLabel ) {
        // match -> project -> create
        List<NeoStatement> statements = new ArrayList<>();
        for ( RexNode projectProject : project.getProjects() ) {
            Translator translator = new Translator( project.getTupleType(), project.getInput().getTupleType(), new HashMap<>(), null, mappingLabel, true );
            statements.add( literal_( PolyString.of( projectProject.accept( translator ) ) ) );
        }
        return statements;
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
        Map<PolyString, PolyString> uuidNameMapping = new HashMap<>();

        List<NeoStatement> statements = new ArrayList<>();

        for ( PolyNode node : nodes ) {
            //String name = "n" + i;
            uuidNameMapping.put( node.id, node.getVariableName() );
            statements.add( node_( node, PolyString.of( implementor.getGraph().mappingLabel ), true ) );
        }
        for ( PolyEdge edge : edges ) {
            statements.add( path_( node_( uuidNameMapping.get( edge.source ) ), edge_( edge, true ), node_( uuidNameMapping.get( edge.target ) ) ) );
        }

        return statements;
    }


    private void handleUpdate( NeoGraphImplementor implementor ) {
        List<NeoStatement> ops = new ArrayList<>();
        for ( RexNode rexNode : operations ) {
            Translator translator = new Translator( getTupleType(), implementor.getLast().getTupleType(), new HashMap<>(), null, implementor.getGraph().mappingLabel, false );
            ops.add( literal_( PolyString.of( rexNode.accept( translator ) ) ) );
        }

        implementor.add( set_( list_( ops ) ) );
    }


    private void handleDelete( NeoGraphImplementor implementor ) {
        List<LiteralStatement> fieldNames = ids.stream().map( NeoStatements::literal_ ).toList();
        implementor.add( delete_( true, list_( fieldNames ) ) );
    }

}

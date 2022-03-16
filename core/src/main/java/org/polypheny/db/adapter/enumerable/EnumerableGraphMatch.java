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

package org.polypheny.db.adapter.enumerable;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.GraphMatch;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.FlatLists;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

public class EnumerableGraphMatch extends GraphMatch implements EnumerableAlg {


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected EnumerableGraphMatch( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<RexNode> matches, List<String> names ) {
        super( cluster, traits, input, matches, names );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        Result res = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        BlockBuilder builder = new BlockBuilder();

        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        final PhysType physType = PhysTypeImpl.of( typeFactory, input.getRowType(), pref.prefer( res.format ) );

        //
        Type inputJavaType = physType.getJavaRowType();

        Type outputJavaType = Object[].class;

        if ( matches.size() == 1 ) {
            outputJavaType = typeFactory.getJavaClass( matches.get( 0 ).getType() );
        }

        //ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), res.block, false );

        Expression inputEnumerator = builder.append( builder.newName( "enumerator" + System.nanoTime() ), Expressions.call( inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ), false );
        builder.add( Expressions.statement( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) );

        Expression graph_ = builder.append( builder.newName( "graph" + System.nanoTime() ), Expressions.convert_( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), PolyGraph.class ), false );

        Expression input = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        InputGetterImpl getter = new InputGetterImpl( Collections.singletonList( Pair.of( input, res.physType ) ) );

        Expression field = getter.field( builder, 0, null );

        List<Expression> expressions = new ArrayList<>( matches.size() );
        for ( RexNode match : matches ) {
            assert match instanceof RexCall;

            RexCall call = ((RexCall) match);

            boolean extract = false;
            Method method;
            switch ( call.op.getOperatorName() ) {
                case CYPHER_NODE_EXTRACT:
                    extract = true;
                    method = BuiltInMethod.GRAPH_NODE_EXTRACT.method;
                    break;
                case CYPHER_NONE_MATCH:
                    method = BuiltInMethod.GRAPH_NODE_MATCH.method;
                    break;
                case CYPHER_PATH_MATCH:
                    method = BuiltInMethod.GRAPH_PATH_MATCH.method;
                    break;
                default:
                    throw new RuntimeException( "could not translate graph match" );
            }

            if ( extract ) {
                expressions.add( Expressions.call( Types.of( Enumerable.class, typeFactory.getJavaClass( call.type ) ), null, method, List.of( graph_ ) ) );
            } else {
                expressions.add( Expressions.call( Types.of( Enumerable.class, typeFactory.getJavaClass( call.type ) ), null, method, List.of( graph_, getPolyElement( call ) ) ) );
            }

        }

        if ( expressions.size() == 1 ) {
            builder.add(
                    Expressions.return_(
                            null,
                            expressions.get( 0 )
                    )
            );
        } else {
            // we have to join ( cross product ) all results together
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.new_(
                                    BuiltInMethod.GRAPH_MATCH_CTOR.constructor,
                                    Expressions.call( List.class, "of", expressions )
                            )
                    )
            );
        }

        return implementor.result( PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( res.format ) ), builder.toBlock() );
    }


    private Expression getPolyElement( RexCall call ) {
        RexNode el = call.operands.get( 1 );
        if ( el.getType().getPolyType() == PolyType.NODE ) {
            PolyNode node = (PolyNode) ((RexLiteral) el).getValue();

            return node.getAsExpression();

        } else if ( el.getType().getPolyType() == PolyType.PATH ) {
            PolyPath path = (PolyPath) ((RexLiteral) el).getValue();

            return path.getAsExpression();
        }

        throw new RuntimeException( "Could not generate expression for graph match." );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + matches.hashCode() + "$" + names.hashCode();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MATCH;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableGraphMatch( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), matches, names );
    }


    public static class MatchEnumerable extends AbstractEnumerable<Object> {

        final List<Enumerable<Object>> enumerables;


        public MatchEnumerable( List<Enumerable<Object>> enumerables ) {
            this.enumerables = enumerables;
        }


        @Override
        public Enumerator<Object> enumerator() {
            Iterator<Enumerable<Object>> iter = enumerables.iterator();
            Enumerable<Object> enumerable = iter.next();
            int i = 0;
            while ( iter.hasNext() ) {
                if ( i == 0 ) {
                    enumerable = enumerable.join( iter.next(), a0 -> a0, a0 -> a0, FlatLists::of );
                } else {
                    enumerable = enumerable.join( iter.next(), a0 -> a0, a0 -> a0,
                            List::of );
                }
                i++;
            }

            return enumerable.enumerator();
        }

    }

}

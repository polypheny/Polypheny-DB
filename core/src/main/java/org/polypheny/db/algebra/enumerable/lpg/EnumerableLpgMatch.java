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

package org.polypheny.db.algebra.enumerable.lpg;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.Types.ArrayType;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableLpgMatch extends LpgMatch implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits The traits of the algebra
     * @param input Input relational expression
     */
    protected EnumerableLpgMatch( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<RexCall> matches, List<PolyString> names ) {
        super( cluster, traits, input, matches, names );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        Result res = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        BlockBuilder builder = new BlockBuilder();

        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), res.block(), false );

        Expression inputEnumerator = builder.append( builder.newName( "enumerator" + System.nanoTime() ), Expressions.call( inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ), false );
        builder.add( Expressions.statement( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) );

        Expression graph_ = builder.append(
                builder.newName( "graph" + System.nanoTime() ),
                Expressions.convert_(
                        Expressions.arrayIndex(
                                Expressions.convert_(
                                        Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), new ArrayType( PolyValue.class ) ),
                                Expressions.constant( 0 ) ),
                        PolyGraph.class ),
                false );

        List<Expression> expressions = new ArrayList<>( matches.size() );
        for ( RexCall match : matches ) {
            assert match != null;

            boolean extract = false;
            Method method = switch ( match.op.getOperatorName() ) {
                case CYPHER_NODE_EXTRACT -> {
                    extract = true;
                    yield BuiltInMethod.CYPHER_NODE_EXTRACT.method;
                }
                case CYPHER_NODE_MATCH -> BuiltInMethod.CYPHER_NODE_MATCH.method;
                case CYPHER_PATH_MATCH -> BuiltInMethod.CYPHER_PATH_MATCH.method;
                default -> throw new GenericRuntimeException( "could not translate graph match" );
            };

            Expression expression;
            if ( extract ) {
                expression = Expressions.call( Types.of( Enumerable.class, typeFactory.getJavaClass( match.type ) ), null, method, List.of( graph_ ) );
            } else {
                expression = Expressions.call( Types.of( Enumerable.class, typeFactory.getJavaClass( match.type ) ), null, method, List.of( graph_, getPolyElement( match ) ) );
            }
            expressions.add( Expressions.call( BuiltInMethod.SINGLE_TO_ARRAY_ENUMERABLE.method, expression ) );

        }
        Expression return_;
        if ( expressions.size() == 1 ) {
            return_ = expressions.get( 0 );
        } else {
            // We have to join ( cross product ) all results together
            return_ = Expressions.new_(
                    BuiltInMethod.CYPHER_MATCH_CTOR.constructor,
                    Expressions.call( Arrays.class, "asList", expressions ) );
        }

        builder.add( Expressions.return_( null, return_ ) );

        return implementor.result( PhysTypeImpl.of( typeFactory, getTupleType(), pref.prefer( res.format() ) ), builder.toBlock() );
    }


    private Expression getPolyElement( RexCall call ) {
        RexNode el = call.operands.get( 1 );
        if ( el.getType().getPolyType() == PolyType.NODE ) {
            PolyNode node = ((RexLiteral) el).getValue().asNode();

            return node.asExpression();

        } else if ( el.getType().getPolyType() == PolyType.PATH ) {
            PolyPath path = ((RexLiteral) el).getValue().asPath();

            return path.asExpression();
        }

        throw new GenericRuntimeException( "Could not generate expression for graph match." );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableLpgMatch( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), matches, names );
    }


    public static class MatchEnumerable extends AbstractEnumerable<PolyValue[]> {

        final List<Enumerable<PolyValue[]>> enumerables;


        public MatchEnumerable( List<Enumerable<PolyValue[]>> enumerables ) {
            this.enumerables = enumerables;
        }


        @Override
        public Enumerator<PolyValue[]> enumerator() {
            Iterator<Enumerable<PolyValue[]>> iter = enumerables.iterator();
            Enumerable<PolyValue[]> enumerable = iter.next();

            int i = 0;
            while ( iter.hasNext() ) {

                int index = i;
                enumerable = enumerable.hashJoin(
                        iter.next(),
                        a0 -> PolyList.of(),
                        a0 -> PolyList.of(),
                        ( a0, a1 ) -> asObjectArray( a0, a1[0], index ) );

                i++;
            }

            return enumerable.enumerator();
        }

    }


    public static PolyValue[] asObjectArray( PolyValue[] a0, PolyValue a1, int index ) {
        PolyValue[] array = new PolyValue[index + 2];
        if ( index + 1 >= 0 ) {
            System.arraycopy( a0, 0, array, 0, index + 1 );
        }
        array[index + 1] = a1;
        return array;
    }

}

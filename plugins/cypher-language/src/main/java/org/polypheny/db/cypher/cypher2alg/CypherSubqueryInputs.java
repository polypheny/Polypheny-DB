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

package org.polypheny.db.cypher.cypher2alg;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgJoin;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.ImmutableBitSet;


/** Lowers row-scoped Cypher EXISTS expressions after MATCH has resolved binding positions. */
final class CypherSubqueryInputs extends AlgShuttleImpl {

    private final AlgCluster cluster;
    private final RexBuilder rexBuilder;


    CypherSubqueryInputs( AlgCluster cluster ) {
        this.cluster = cluster;
        this.rexBuilder = cluster.getRexBuilder();
    }


    @Override
    public AlgNode visit( LogicalLpgProject project ) {
        AlgNode input = project.getInput().accept( this );
        ExistsReference references = new ExistsReference( input );
        List<RexNode> values = project.getProjects().stream().map( value -> value.accept( references ) ).toList();
        return references.rows == project.getInput() ? project : LogicalLpgProject.create( references.rows, values, project.getNames() );
    }


    @Override
    public AlgNode visit( LogicalLpgFilter filter ) {
        AlgNode input = filter.getInput().accept( this );
        ExistsReference references = new ExistsReference( input );
        RexNode condition = filter.getCondition().accept( references );
        if ( references.rows == input ) {
            return input == filter.getInput() ? filter : filter.copy( filter.getTraitSet(), List.of( input ) );
        }
        AlgNode result = LogicalLpgFilter.create( references.rows, condition );
        return LogicalLpgProject.create( result, columns( result, 0, width( input ) ), names( input ) );
    }


    @Override
    public AlgNode visit( AlgNode node ) {
        if ( !(node instanceof LogicalLpgJoin join) || !containsExists( join.getCondition() ) ) {
            return super.visit( node );
        }
        AlgNode left = join.getLeft();
        AlgNode right = join.getRight();
        if ( join.getJoinType() == JoinAlgType.LEFT ) {
            // Apply the predicate to right-hand candidates before OPTIONAL MATCH introduces a null row.
            CorrelationId id = cluster.createCorrel();
            RexNode outer = rexBuilder.makeCorrel( left.getTupleType(), id );
            RexNode condition = join.getCondition().accept( new RexShuttle() {
                @Override
                public RexNode visitIndexRef( RexIndexRef ref ) {
                    return ref.getIndex() < width( left )
                            ? rexBuilder.makeFieldAccess( outer, ref.getIndex() )
                            : rexBuilder.makeInputRef( ref.getType(), ref.getIndex() - width( left ) );
                }
            } );
            AlgNode candidates = LogicalLpgFilter.create( right, condition ).accept( this );
            return new LogicalLpgCall( left.accept( this ), candidates, id, ImmutableBitSet.range( width( left ) ), SemiJoinType.LEFT );
        }
        List<RexNode> regular = new ArrayList<>();
        List<RexNode> subqueries = new ArrayList<>();
        for ( RexNode conjunct : AlgOptUtil.conjunctions( join.getCondition() ) ) {
            (containsExists( conjunct ) ? subqueries : regular).add( conjunct );
        }
        AlgNode candidates = LogicalLpgJoin.create( left, right, RexUtil.composeConjunction( rexBuilder, regular ), join.getJoinType() );
        return LogicalLpgFilter.create( candidates, RexUtil.composeConjunction( rexBuilder, subqueries ) ).accept( this );
    }


    private static boolean containsExists( RexNode node ) {
        return node instanceof RexCall call && (call instanceof Exists || call.operands.stream().anyMatch( CypherSubqueryInputs::containsExists ));
    }


    private int width( AlgNode input ) {
        return input.getTupleType().getFieldCount();
    }


    private List<PolyString> names( AlgNode input ) {
        return input.getTupleType().getFieldNames().stream().map( PolyString::of ).toList();
    }


    private List<RexNode> columns( AlgNode input, int start, int count ) {
        return IntStream.range( start, start + count ).mapToObj( index -> (RexNode) rexBuilder.makeInputRef( input, index ) ).toList();
    }


    /** A temporary Rex expression that carries the subquery and its row captures through scope rewrites. */
    static final class Exists extends RexCall {

        private final AlgNode body;
        private final CorrelationId correlation;


        Exists( AlgDataType type, List<RexNode> operands, AlgNode body, CorrelationId correlation ) {
            super( type, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXISTS_SUBQUERY ), operands );
            this.body = body;
            this.correlation = correlation;
        }


        @Override
        public RexCall clone( AlgDataType type, List<RexNode> operands ) {
            return new Exists( type, operands, body, correlation );
        }


        @Override
        protected @Nonnull String computeDigest( boolean withType ) {
            return super.computeDigest( withType ) + "[" + correlation + "]";
        }

    }


    private final class ExistsReference extends RexShuttle {

        private final Map<Exists, RexNode> lowered = new IdentityHashMap<>();
        private AlgNode rows;


        private ExistsReference( AlgNode rows ) {
            this.rows = rows;
        }


        @Override
        public RexNode visitCall( RexCall call ) {
            if ( !(call instanceof Exists subquery) ) {
                return super.visitCall( call );
            }
            // A sort predicate can capture a projected alias containing this same subquery.
            RexNode existing = lowered.get( subquery );
            if ( existing != null ) {
                return existing;
            }
            List<RexNode> projected = new ArrayList<>( subquery.operands.stream().map( operand -> operand.accept( this ) ).toList() );
            int captures = projected.size();
            int originalWidth = width( rows );
            List<PolyString> originalNames = names( rows );
            List<PolyString> projectedNames = new ArrayList<>();
            for ( int i = 0; i < captures; i++ ) {
                projectedNames.add( PolyString.of( "$existsInput" + i ) );
            }
            projected.addAll( columns( rows, 0, originalWidth ) );
            projectedNames.addAll( originalNames );
            AlgNode outer = LogicalLpgProject.create( rows, projected, projectedNames );

            AlgNode first = LogicalLpgSort.create( AlgCollations.EMPTY, subquery.body.accept( CypherSubqueryInputs.this ), null, 1L );
            AlgNode marker = LogicalLpgProject.create( first, List.of( rexBuilder.makeLiteral( true ) ), List.of( PolyString.of( "$existsMarker" ) ) );
            AlgNode joined = new LogicalLpgCall( outer, marker, subquery.correlation, ImmutableBitSet.range( captures ), SemiJoinType.LEFT );
            RexNode present = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), rexBuilder.makeInputRef( joined, width( outer ) ) );
            List<RexNode> visible = new ArrayList<>( columns( joined, captures, originalWidth ) );
            visible.add( present );
            List<PolyString> visibleNames = new ArrayList<>( originalNames );
            visibleNames.add( PolyString.of( "$exists" + originalWidth ) );
            rows = LogicalLpgProject.create( joined, visible, visibleNames );
            RexNode reference = rexBuilder.makeInputRef( rows, originalWidth );
            lowered.put( subquery, reference );
            return reference;
        }

    }

}

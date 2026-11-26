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

package org.polypheny.db.algebra.logical.lpg;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.functions.CypherShortestPathRows;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.util.ValidatorUtil;


/** Applies the MATCH predicates before selecting shortest paths or emitting an optional null row. */
@Getter
public class LogicalLpgShortestPathJoin extends BiAlg implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgShortestPathJoin.class, node -> true, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgShortestPathJoinToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgShortestPathJoin join = (LogicalLpgShortestPathJoin) node;
            AlgNode left = convert( join.left, join.left.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            AlgNode right = convert( join.right, join.right.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            return new EnumerableShortestPathJoin( join.getTraitSet().replace( EnumerableConvention.INSTANCE ), left, right,
                    join.condition, join.optional, join.pathColumns, join.groupColumns, join.allTies );
        }
    };

    private final RexNode condition;
    private final boolean optional;
    private final List<Integer> pathColumns;
    private final List<Integer> groupColumns;
    private final List<Boolean> allTies;


    public LogicalLpgShortestPathJoin( AlgNode left, AlgNode right, RexNode condition, boolean optional, List<Integer> pathColumns,
            List<Integer> groupColumns, List<Boolean> allTies ) {
        this( left.getTraitSet(), left, right, condition, optional, pathColumns, groupColumns, allTies );
    }


    protected LogicalLpgShortestPathJoin( AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, boolean optional,
            List<Integer> pathColumns, List<Integer> groupColumns, List<Boolean> allTies ) {
        super( left.getCluster(), traits, left, right );
        this.condition = condition;
        this.optional = optional;
        this.pathColumns = List.copyOf( pathColumns );
        this.groupColumns = List.copyOf( groupColumns );
        this.allTies = List.copyOf( allTies );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.JOIN;
    }


    @Override
    protected AlgDataType deriveRowType() {
        return ValidatorUtil.deriveJoinRowType( left.getTupleType(), right.getTupleType(), optional ? JoinAlgType.LEFT : JoinAlgType.INNER,
                getCluster().getTypeFactory(), null );
    }


    @Override
    public List<RexNode> getChildExps() {
        return List.of( condition );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode rewritten = shuttle.apply( condition );
        if ( rewritten == condition ) {
            return this;
        }
        return withInputsAndCondition( traitSet, left, right, rewritten );
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return withInputsAndCondition( traits, inputs.get( 0 ), inputs.get( 1 ), condition );
    }


    protected LogicalLpgShortestPathJoin withInputsAndCondition( AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition ) {
        return new LogicalLpgShortestPathJoin( traits, left, right, condition, optional, pathColumns, groupColumns, allTies );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        return super.explainTerms( writer ).item( "condition", condition ).item( "optional", optional )
                .item( "pathColumns", pathColumns ).item( "groupColumns", groupColumns ).item( "allTies", allTies );
    }


    private static final class EnumerableShortestPathJoin extends LogicalLpgShortestPathJoin implements EnumerableAlg {

        private EnumerableShortestPathJoin( AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition, boolean optional,
                List<Integer> pathColumns, List<Integer> groupColumns, List<Boolean> allTies ) {
            super( traits, left, right, condition, optional, pathColumns, groupColumns, allTies );
        }


        @Override
        protected LogicalLpgShortestPathJoin withInputsAndCondition( AlgTraitSet traits, AlgNode left, AlgNode right, RexNode condition ) {
            return new EnumerableShortestPathJoin( traits, left, right, condition, isOptional(), getPathColumns(), getGroupColumns(), getAllTies() );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            BlockBuilder builder = new BlockBuilder();
            Result leftChild = implementor.visitChild( this, 0, (EnumerableAlg) left, Prefer.ARRAY );
            Result rightChild = implementor.visitChild( this, 1, (EnumerableAlg) right, Prefer.ARRAY );
            PhysType leftType = rowArray( implementor, left.getTupleType() );
            PhysType rightType = rowArray( implementor, right.getTupleType() );
            Expression leftRows = leftChild.physType().convertTo( builder.append( "outerRows", leftChild.block() ), leftType );
            Expression rightRows = rightChild.physType().convertTo( builder.append( "candidateRows", rightChild.block() ), rightType );
            Expression matches = EnumUtils.generatePredicate( implementor, getCluster().getRexBuilder(), left, right, leftType, rightType, getCondition() );
            Expression allTies = Expressions.newArrayInit( boolean.class,
                    getAllTies().stream().map( flag -> (Expression) Expressions.constant( flag, boolean.class ) ).toList() );
            Expression output = Expressions.call( CypherShortestPathRows.class, "join", leftRows, rightRows, matches,
                    Expressions.constant( isOptional() ), Expressions.constant( right.getTupleType().getFieldCount() ),
                    indexes( getPathColumns() ), indexes( getGroupColumns() ), allTies );
            builder.add( Expressions.return_( null, output ) );
            return implementor.result( rowArray( implementor, getTupleType() ), builder.toBlock() );
        }


        private static Expression indexes( List<Integer> columns ) {
            return Expressions.newArrayInit( int.class, columns.stream().map( i -> (Expression) Expressions.constant( i ) ).toList() );
        }


        private static PhysType rowArray( EnumerableAlgImplementor implementor, AlgDataType rowType ) {
            return PhysTypeImpl.of( implementor.getTypeFactory(), rowType, JavaTupleFormat.ARRAY, false );
        }
    }
}

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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;

/**
 * Parquet-convention join for supported parent/child table joins.
 */
public class ParquetJoin extends Join implements ParquetAlg {

    private final boolean leftIsParent;
    private final ParquetRelTable leftTable;
    private final ParquetRelTable rightTable;
    private final int[] leftFields;
    private final int[] rightFields;
    private final List<ParquetAdapterFilter> filters;


    public ParquetJoin(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            AlgNode left,
            AlgNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinAlgType joinType,
            boolean leftIsParent,
            ParquetRelTable leftTable,
            ParquetRelTable rightTable,
            int[] leftFields,
            int[] rightFields,
            List<ParquetAdapterFilter> filters ) {
        super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        this.leftIsParent = leftIsParent;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.filters = List.copyOf( filters );
    }


    public static ParquetJoin create( ParquetScan left, ParquetScan right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean leftIsParent ) {
        AlgCluster cluster = left.getCluster();
        AlgTraitSet traitSet = cluster.traitSetOf( ParquetConvention.INSTANCE ).replace( ModelTrait.RELATIONAL );
        return new ParquetJoin(
                cluster,
                traitSet,
                left,
                right,
                condition,
                variablesSet,
                joinType,
                leftIsParent,
                left.getEntity(),
                right.getEntity(),
                left.getFields(),
                right.getFields(),
                joinInputFilters( left, right ) );
    }


    private static List<ParquetAdapterFilter> joinInputFilters( ParquetScan leftScan, ParquetScan rightScan ) {
        List<ParquetAdapterFilter> filters = new ArrayList<>( leftScan.getFilters().size() + rightScan.getFilters().size() );
        leftScan.getFilters().forEach( filter -> filters.add( shiftFilter( filter, 0 ) ) );
        rightScan.getFilters().forEach( filter -> filters.add( shiftFilter( filter, leftScan.getFields().length ) ) );
        return filters;
    }


    private static ParquetAdapterFilter shiftFilter( ParquetAdapterFilter filter, int offset ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> shiftFilter( operand, offset ) )
                    .toList() );
        }
        return new ParquetAdapterFilter(
                filter.columnIndex() + offset,
                filter.pathElements(),
                filter.operator(),
                filter.polyValue(),
                filter.dynamicParamIndex() );
    }


    /**
     * Checks whether a join can be executed by the Parquet adapter: it must be join
     * between two tables from the same Parquet source, using parent PRIMARY_KEY and child PARENT_KEY.
     */
    public static JoinDirection supportedDirection( Join join, ParquetScan left, ParquetScan right ) {
        return supportedDirection( join.getCondition(), join.getJoinType(), left, right );
    }


    /**
     * Checks whether a join can be executed by the Parquet adapter: it must be join
     * between two tables from the same Parquet source, using parent PRIMARY_KEY and child PARENT_KEY.
     */
    public static JoinDirection supportedDirection( RexNode condition, JoinAlgType joinType, ParquetScan left, ParquetScan right ) {
        JoinInfo info = JoinInfo.of( left, right, condition );
        if ( !info.isEqui() || info.leftKeys.size() != 1 || info.rightKeys.size() != 1 ) {
            return null;
        }

        ParquetRelTable leftTable = left.getEntity();
        ParquetRelTable rightTable = right.getEntity();
        if ( !leftTable.getSourceUrl().equals( rightTable.getSourceUrl() ) ) {
            return null;
        }

        int leftColumn = mappedColumn( left.getFields(), info.leftKeys.get( 0 ), left.getEntity().getFieldCount() );
        int rightColumn = mappedColumn( right.getFields(), info.rightKeys.get( 0 ), right.getEntity().getFieldCount() );
        if ( leftColumn < 0 || rightColumn < 0 ) {
            return null;
        }

        boolean leftIsParent = isParentChildJoin( leftTable, rightTable, leftColumn, rightColumn );
        boolean rightIsParent = isParentChildJoin( rightTable, leftTable, rightColumn, leftColumn );
        if ( !leftIsParent && !rightIsParent ) {
            return null;
        }

        switch ( joinType ) {
            case INNER, FULL, LEFT, RIGHT -> {
                return new JoinDirection( leftIsParent );
            }
            default -> {
                return null;
            }
        }
    }


    private static int mappedColumn( int[] fields, int inputIndex, int totalFieldsCount ) {
        if ( inputIndex < 0 || inputIndex >= fields.length ) {
            return -1;
        }
        int column = fields[inputIndex];
        return column < 0 || column >= totalFieldsCount ? -1 : column;
    }


    private static boolean isParentChildJoin( ParquetRelTable parent, ParquetRelTable child, int parentColumn, int childColumn ) {
        if ( !isDirectChildPath( parent.getBinding().sourcePathElements(), child.getBinding().sourcePathElements() ) ) {
            return false;
        }
        return parentColumn == parent.columnIndexByRole( ParquetColumnRole.PRIMARY_KEY ) && childColumn == child.columnIndexByRole( ParquetColumnRole.PARENT_KEY );
    }


    private static boolean isDirectChildPath( List<String> parentPath, List<String> childPath ) {
        return childPath.size() == parentPath.size() + 1 && childPath.subList( 0, parentPath.size() ).equals( parentPath );
    }


    public ParquetJoin withFilters( List<ParquetAdapterFilter> filters ) {
        List<ParquetAdapterFilter> combinedFilters = new ArrayList<>( this.filters );
        combinedFilters.addAll( filters );
        return new ParquetJoin( getCluster(), traitSet, left, right, condition, variablesSet, joinType, leftIsParent, leftTable, rightTable, leftFields, rightFields, combinedFilters );
    }


    @Override
    public Join copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        return new ParquetJoin( getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType, leftIsParent, leftTable, rightTable, leftFields, rightFields, filters );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        List<String> joinedFields = ParquetPolyAlgDisplay.joinedFieldNames( leftTable, leftFields, rightTable, rightFields );
        return super.bindArguments()
                .put( "leftIsParent", new BooleanArg( leftIsParent ) )
                .put( "leftFields", new ListArg<>( ParquetPolyAlgDisplay.fieldNames( leftTable, leftFields ), StringArg::new ) )
                .put( "rightFields", new ListArg<>( ParquetPolyAlgDisplay.fieldNames( rightTable, rightFields ), StringArg::new ) )
                .put( "filters", new ListArg<>( ParquetPolyAlgDisplay.filters( filters, joinedFields ), StringArg::new ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        Optional<Double> count = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( count.orElse( estimateTupleCount( mq ) ), 0, 0 ).multiplyBy( ParquetConvention.COST_MULTIPLIER );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return Math.max( left.estimateTupleCount( mq ), right.estimateTupleCount( mq ) );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "leftIsParent", leftIsParent )
                .item( "filters", filters );
    }


    @Override
    public Expression implement( EnumerableAlgImplementor implementor ) {
        boolean emitUnmatchedParents = emitUnmatchedParents();
        Expression runtimeFilters = EnumUtils.expressionList( filters.stream().map( ParquetAdapterFilter::toExpression ).toList() );

        return Expressions.call(
                leftTable.asExpression( ParquetRelTable.class ),
                "nestedJoin",
                implementor.getRootExpression(),
                rightTable.asExpression( ParquetRelTable.class ),
                Expressions.constant( leftFields ),
                Expressions.constant( rightFields ),
                Expressions.constant( leftIsParent ),
                Expressions.constant( emitUnmatchedParents ),
                runtimeFilters );
    }


    private boolean emitUnmatchedParents() {
        return switch ( joinType ) {
            case LEFT -> leftIsParent;
            case RIGHT -> !leftIsParent;
            case FULL -> true;
            default -> false;
        };
    }

}

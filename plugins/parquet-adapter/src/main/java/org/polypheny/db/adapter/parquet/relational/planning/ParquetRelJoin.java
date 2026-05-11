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
import org.apache.calcite.linq4j.tree.Blocks;
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
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;

/**
 * Parquet physical join for supported nested parent/child table joins.
 */
public class ParquetRelJoin extends Join implements EnumerableAlg {

    private final boolean leftIsParent;
    private final ParquetRelTable leftTable;
    private final ParquetRelTable rightTable;
    private final int[] leftFields;
    private final int[] rightFields;
    private final List<ParquetAdapterFilter> filters;
    private final JoinInputLimit parentLimit;


    public ParquetRelJoin(
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
            List<ParquetAdapterFilter> filters,
            JoinInputLimit parentLimit ) {
        super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        this.leftIsParent = leftIsParent;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftFields = leftFields;
        this.rightFields = rightFields;
        this.filters = List.copyOf( filters );
        this.parentLimit = parentLimit;
    }


    public static ParquetRelJoin create( ParquetRelScan left, ParquetRelScan right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean leftIsParent ) {
        return create( left, right, left, right, condition, variablesSet, joinType, leftIsParent, JoinInputLimit.NONE );
    }


    /**
     * factory method that builds the physical adapter join node
     */
    public static ParquetRelJoin create( AlgNode leftInput, AlgNode rightInput, ParquetRelScan leftScan, ParquetRelScan rightScan, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean leftIsParent, JoinInputLimit parentLimit ) {
        AlgCluster cluster = leftInput.getCluster();
        AlgTraitSet traitSet = cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ModelTrait.RELATIONAL );
        return new ParquetRelJoin(
                cluster,
                traitSet,
                leftInput,
                rightInput,
                condition,
                variablesSet,
                joinType,
                leftIsParent,
                leftScan.getEntity(),
                rightScan.getEntity(),
                leftScan.getFields(),
                rightScan.getFields(),
                List.of(),
                parentLimit );
    }


    /**
     * Checks whether a join can be executed by the Parquet adapter: it must be join
     * between two tables from the same Parquet source, using parent PRIMARY_KEY and child PARENT_KEY
     */
    public static JoinDirection supportedDirection( Join join, ParquetRelScan left, ParquetRelScan right ) {
        JoinInfo info = JoinInfo.of( left, right, join.getCondition() );
        if ( !info.isEqui() || info.leftKeys.size() != 1 || info.rightKeys.size() != 1 ) {
            return null;
        }

        ParquetRelTable leftTable = left.getEntity();
        ParquetRelTable rightTable = right.getEntity();
        if ( !leftTable.getSourceUrl().equals( rightTable.getSourceUrl() ) ) {
            return null;
        }

        int leftColumn = mappedColumn( left, info.leftKeys.get( 0 ) );
        int rightColumn = mappedColumn( right, info.rightKeys.get( 0 ) );
        if ( leftColumn < 0 || rightColumn < 0 ) {
            return null;
        }

        boolean leftIsParent = isParentChildJoin( leftTable, rightTable, leftColumn, rightColumn );
        boolean rightIsParent = isParentChildJoin( rightTable, leftTable, rightColumn, leftColumn );
        if ( !leftIsParent && !rightIsParent ) {
            return null;
        }

        switch ( join.getJoinType() ) {
            case INNER, FULL, LEFT, RIGHT -> {
                return new JoinDirection( leftIsParent );
            }
            default -> {
                return null;
            }
        }
    }


    private static int mappedColumn( ParquetRelScan scan, int inputIndex ) {
        int[] fields = scan.getFields();
        if ( inputIndex < 0 || inputIndex >= fields.length ) {
            return -1;
        }
        int column = fields[inputIndex];
        return column < 0 || column >= scan.getEntity().getFieldCount() ? -1 : column;
    }


    private static boolean isParentChildJoin( ParquetRelTable parent, ParquetRelTable child, int parentColumn, int childColumn ) {
        if ( !isDirectChildPath( parent.getBinding().sourcePathElements(), child.getBinding().sourcePathElements() ) ) {
            return false;
        }
        return parentColumn == parent.columnIndexByRole( ParquetColumnRole.PRIMARY_KEY )
                && childColumn == child.columnIndexByRole( ParquetColumnRole.PARENT_KEY );
    }


    public static boolean isDirectChildPath( List<String> parentPath, List<String> childPath ) {
        return childPath.size() == parentPath.size() + 1 && childPath.subList( 0, parentPath.size() ).equals( parentPath );
    }


    public ParquetRelJoin withFilters( List<ParquetAdapterFilter> filters ) {
        List<ParquetAdapterFilter> combinedFilters = new ArrayList<>( this.filters );
        combinedFilters.addAll( filters );
        return new ParquetRelJoin( getCluster(), traitSet, left, right, condition, variablesSet, joinType, leftIsParent, leftTable, rightTable, leftFields, rightFields, combinedFilters, parentLimit );
    }


    @Override
    public Join copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        return new ParquetRelJoin( getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType, leftIsParent, leftTable, rightTable, leftFields, rightFields, filters, parentLimit );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        Optional<Double> count = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( count.orElse( estimateTupleCount( mq ) ), 0, 0 );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "leftIsParent", leftIsParent )
                .itemIf( "parentOffset", parentLimit.offset(), parentLimit.offset() != 0 )
                .itemIf( "parentFetch", parentLimit.fetch(), parentLimit.fetch() >= 0 )
                .item( "filters", filters );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );
        boolean emitUnmatchedParents = emitUnmatchedParents();
        Expression runtimeFilters = EnumUtils.expressionList( filters.stream().map( ParquetAdapterFilter::toExpression ).toList() );

        return implementor.result(
                physType,
                Blocks.toBlock(
                        Expressions.call(
                                leftTable.asExpression( ParquetRelTable.class ),
                                "nestedJoin",
                                implementor.getRootExpression(),
                                rightTable.asExpression( ParquetRelTable.class ),
                                Expressions.constant( leftFields ),
                                Expressions.constant( rightFields ),
                                Expressions.constant( leftIsParent ),
                                Expressions.constant( emitUnmatchedParents ),
                                Expressions.constant( parentLimit.offset() ),
                                Expressions.constant( parentLimit.fetch() ),
                                runtimeFilters ) ) );
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

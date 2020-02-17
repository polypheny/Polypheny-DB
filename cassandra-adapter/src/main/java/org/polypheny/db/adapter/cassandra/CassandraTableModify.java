/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.cassandra.CassandraRel.CassandraImplementContext.Type;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


@Slf4j
public class CassandraTableModify extends TableModify implements CassandraRel {

    public final CassandraTable cassandraTable;


    /**
     * Creates a {@code TableModify}.
     *
     * The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
     * </blockquote>
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param table Target table to modify
     * @param catalogReader accessor to the table metadata.
     * @param input Sub-query or filter condition
     * @param operation Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    public CassandraTableModify(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.cassandraTable = table.unwrap( CassandraTable.class );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new CassandraTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                AbstractRelNode.sole( inputs ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }


    @Override
    public void register( RelOptPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        log.debug( "CTM: Implementing." );
        context.cassandraTable = cassandraTable;
        context.table = table;

        switch ( this.getOperation() ) {
            case INSERT:
                log.debug( "CTM: Insert detected." );
                context.type = Type.INSERT;
                context.visitChild( 0, getInput() );
                break;
            case UPDATE:
                log.debug( "CTM: Update detected." );
                context.type = Type.UPDATE;
                context.visitChild( 0, getInput() );

                List<Assignment> setAssignments = new ArrayList<>();
                for ( Pair<String, RexNode> entry : Pair.zip( this.getUpdateColumnList(), this.getSourceExpressionList() ) ) {
                    if ( !(entry.right instanceof RexLiteral) ) {
                        throw new RuntimeException( "Non literal values are not yet supported." );
                    }

                    String physicalColumnName = ((CassandraConvention) getConvention()).physicalNameProvider.getPhysicalColumnName( cassandraTable.getColumnFamily(), entry.left );
                    setAssignments.add( Assignment.setColumn( physicalColumnName, QueryBuilder.literal( CassandraValues.literalValue( (RexLiteral) entry.right ) ) ) );
                }

                context.addAssignments( setAssignments );

                break;
            case DELETE:
                log.debug( "CTM: Delete detected." );
                context.type = Type.DELETE;
                context.visitChild( 0, getInput() );
                break;
            case MERGE:
                throw new RuntimeException( "Merge is not supported." );
        }
    }
}

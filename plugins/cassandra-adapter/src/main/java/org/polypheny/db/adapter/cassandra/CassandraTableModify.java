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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.cassandra.CassandraAlg.CassandraImplementContext.Type;
import org.polypheny.db.adapter.cassandra.util.CassandraTypesUtils;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.util.Pair;


@Slf4j
public class CassandraTableModify extends Modify implements CassandraAlg {

    public final CassandraTable cassandraTable;


    /**
     * Creates a {@code Modify}.
     * <p>
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
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.cassandraTable = table.unwrap( CassandraTable.class );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new CassandraTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                AbstractAlgNode.sole( inputs ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }


    @Override
    public void register( AlgOptPlanner planner ) {
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
                    if ( !(entry.right instanceof RexLiteral) && !((entry.right instanceof RexCall) && (((RexCall) entry.right).getOperator() instanceof SqlArrayValueConstructor)) ) {
                        throw new RuntimeException( "Non literal values are not yet supported." );
                    }

                    String physicalColumnName = ((CassandraConvention) getConvention()).physicalNameProvider.getPhysicalColumnName( cassandraTable.getColumnFamily(), entry.left );

                    Term term;
                    if ( entry.right instanceof RexLiteral ) {
                        term = QueryBuilder.literal( CassandraValues.literalValue( (RexLiteral) entry.right ) );
                    } else {
                        SqlArrayValueConstructor arrayValueConstructor = (SqlArrayValueConstructor) ((RexCall) entry.right).op;
                        UdtValue udtValue = CassandraTypesUtils.createArrayContainerDataType(
                                context.cassandraTable.getUnderlyingConvention().arrayContainerUdt,
                                arrayValueConstructor.dimension,
                                arrayValueConstructor.maxCardinality,
                                ((RexCall) entry.right).type.getComponentType().getPolyType(),
                                (RexCall) entry.right );
                        String udtString = udtValue.getFormattedContents();
                        term = QueryBuilder.raw( udtString );
                    }
                    setAssignments.add( Assignment.setColumn( physicalColumnName, term ) );
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

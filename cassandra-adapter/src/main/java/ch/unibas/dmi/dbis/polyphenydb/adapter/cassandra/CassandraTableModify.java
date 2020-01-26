/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraRel.CassandraImplementContext.Type;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.CatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CassandraTableModify extends TableModify implements CassandraRel {

    public final CassandraTable cassandraTable;
    private final String columnFamily;

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
    public CassandraTableModify( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.cassandraTable = table.unwrap( CassandraTable.class );
        this.columnFamily = this.cassandraTable.getColumnFamily();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
//        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return new CassandraTableModify( getCluster(), traitSet, getTable(), getCatalogReader(), AbstractRelNode.sole( inputs ), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened() );
    }

    @Override
    public void register( RelOptPlanner planner ) {
        getConvention().register( planner );
//        planner.addRule( CassandraToEnumerableConverterRule.INSTANCE );
//        for ( RelOptRule rule : CassandraRules.RULES ) {
//            planner.addRule( rule );
//        }
    }

    @Override
    public void implement( CassandraImplementContext context ) {
        log.debug( "CTM: Implementing." );
//        implementor.visitChild( 0, getInput() );
        context.cassandraTable = cassandraTable;
        context.table = table;

        switch ( this.getOperation() ) {
            case INSERT:
                log.debug( "CTM: Insert detected." );
                context.type = Type.INSERT;
                context.visitChild( 0, getInput() );

                if ( context.insertValues.size() == 1 ) {
                    log.info( "CTM: Simple Insert detected." );
                    InsertInto insertInto = QueryBuilder.insertInto( this.columnFamily );
                    RegularInsert insert = insertInto.values( context.insertValues.get( 0 ) );

                } else {
                    // TODO JS: I don't like this solution, but for now it'll do!
                    log.debug( "CTM: Batch Insert detected." );
//                    BatchStatementBuilder builder = new BatchStatementBuilder( BatchType.LOGGED );
                    List<SimpleStatement> statements = new ArrayList<>(  );

                    for ( Map<String, Term> insertValue: context.insertValues ) {
                        InsertInto insertInto = QueryBuilder.insertInto( this.columnFamily );

                        statements.add( insertInto.values( insertValue ).build() );
//                        builder.addStatement( insertInto.values( insertValue ).build() );
                    }

//                    context.addState( statements );
//                    implementor.batchStatement = builder.build();
                }
                break;
            case UPDATE:
                log.debug( "CTM: Update detected." );
                context.type = Type.UPDATE;
                context.visitChild( 0, getInput() );

//                updateStart.set


                List<Assignment> setAssignments = new ArrayList<>();
                for(Pair<String, RexNode> entry :Pair.zip(this.getUpdateColumnList(), this.getSourceExpressionList())) {
                    if ( ! ( entry.right instanceof RexLiteral ) ) {
                        throw new RuntimeException( "Non literal values are not yet supported." );
                    }
                    setAssignments.add( Assignment.setColumn( entry.left, QueryBuilder.literal( CassandraValues.literalValue( (RexLiteral) entry.right ) ) ) );
                }

                context.addAssignments( setAssignments );


                SimpleStatement updateStart = QueryBuilder.update( this.columnFamily )
                        .set( setAssignments )
                        .where( context.whereClause )
                        .build()
                        ;

//                implementor.simpleStatement = updateStart;
//                UpdateWithAssignments update = updateStart.set( setAssignments );
//                update.where( implementor.whereClause );


                break;
            case DELETE:
                break;
            case MERGE:
                break;
        }
    }
}

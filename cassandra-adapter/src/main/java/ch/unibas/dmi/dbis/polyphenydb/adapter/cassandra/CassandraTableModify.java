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


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraFilter.Translator;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraRel.Implementor.Type;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.CatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CassandraTableModify extends TableModify implements CassandraRel {

    final private CassandraTable cassandraTable;
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
    protected CassandraTableModify( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened, CassandraTable cassandraTable, final String columnFamily ) {
        super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.cassandraTable = cassandraTable;
        this.columnFamily = columnFamily;
    }


    @Override
    public void register( RelOptPlanner planner ) {
        planner.addRule( CassandraToEnumerableConverterRule.INSTANCE );
        for ( RelOptRule rule : CassandraRules.RULES ) {
            planner.addRule( rule );
        }
    }

    @Override
    public void implement( Implementor implementor ) {
        log.info( "CTM: Implementing." );
        implementor.visitChild( 0, getInput() );
        implementor.cassandraTable = cassandraTable;
        implementor.table = table;

        switch ( this.getOperation() ) {
            case INSERT:
                log.info( "CTM: Insert detected." );
                implementor.type = Type.INSERT;
                if ( implementor.insertValues.size() == 1 ) {
                    log.info( "CTM: Simple Insert detected." );
                    InsertInto insertInto = QueryBuilder.insertInto( this.columnFamily );
                    RegularInsert insert = insertInto.values( implementor.insertValues.get( 0 ) );

                    implementor.simpleStatement = insert.build();
                } else {
                    // TODO JS: I don't like this solution, but for now it'll do!
                    log.info( "CTM: Batch Insert detected." );
                    BatchStatementBuilder builder = new BatchStatementBuilder( BatchType.LOGGED );

                    for ( Map<String, Term> insertValue: implementor.insertValues ) {
                        InsertInto insertInto = QueryBuilder.insertInto( this.columnFamily );

                        builder.addStatement( insertInto.values( insertValue ).build() );
                    }

                    implementor.batchStatement = builder.build();
                }
                break;
            case UPDATE:
                break;
            case DELETE:
                break;
            case MERGE:
                break;
        }
    }
}

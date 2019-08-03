/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
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
 */

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptMaterialization;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl.PolyphenyDbPreparingStmt;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCorrelate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalExchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalIntersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalMatch;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalMinus;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalSort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalUnion;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalValues;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlRexConvertletTable;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Context for populating a {@link Prepare.Materialization}.
 */
class PolyphenyDbMaterializer extends PolyphenyDbPreparingStmt {

    PolyphenyDbMaterializer( PolyphenyDbPrepareImpl prepare, Context context, CatalogReader catalogReader, PolyphenyDbSchema schema, RelOptPlanner planner, SqlRexConvertletTable convertletTable ) {
        super( prepare, context, catalogReader, catalogReader.getTypeFactory(), schema, EnumerableRel.Prefer.ANY, planner, BindableConvention.INSTANCE, convertletTable );
    }


    /**
     * Populates a materialization record, converting a table path (essentially a list of strings, like ["hr", "sales"]) into a table object that can be used in the planning process.
     */
    void populate( Materialization materialization ) {
        SqlParser parser = SqlParser.create( materialization.sql );
        SqlNode node;
        try {
            node = parser.parseStmt();
        } catch ( SqlParseException e ) {
            throw new RuntimeException( "parse failed", e );
        }
        final SqlToRelConverter.Config config =
                SqlToRelConverter
                        .configBuilder()
                        .withTrimUnusedFields( true )
                        .build();
        SqlToRelConverter sqlToRelConverter2 = getSqlToRelConverter( getSqlValidator(), catalogReader, config );

        materialization.queryRel = sqlToRelConverter2.convertQuery( node, true, true ).rel;

        // Identify and substitute a StarTable in queryRel.
        //
        // It is possible that no StarTables match. That is OK, but the materialization patterns that are recognized will not be as rich.
        //
        // It is possible that more than one StarTable matches. TBD: should we take the best (whatever that means), or all of them?
        useStar( schema, materialization );

        RelOptTable table = this.catalogReader.getTable( materialization.materializedTable.path() );
        materialization.tableRel = sqlToRelConverter2.toRel( table );
    }


    /**
     * Converts a relational expression to use a {@link StarTable} defined in {@code schema}. Uses the first star table that fits.
     */
    private void useStar( PolyphenyDbSchema schema, Materialization materialization ) {
        for ( Callback x : useStar( schema, materialization.queryRel ) ) {
            // Success -- we found a star table that matches.
            materialization.materialize( x.rel, x.starRelOptTable );
            if ( PolyphenyDbPrepareImpl.DEBUG ) {
                System.out.println( "Materialization " + materialization.materializedTable + " matched star table " + x.starTable + "; query after re-write: " + RelOptUtil.toString( materialization.queryRel ) );
            }
        }
    }


    /**
     * Converts a relational expression to use a {@link ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable} defined in {@code schema}. Uses the first star table that fits.
     */
    private Iterable<Callback> useStar( PolyphenyDbSchema schema, RelNode queryRel ) {
        List<PolyphenyDbSchema.TableEntry> starTables = Schemas.getStarTables( schema.root() );
        if ( starTables.isEmpty() ) {
            // Don't waste effort converting to leaf-join form.
            return ImmutableList.of();
        }
        final List<Callback> list = new ArrayList<>();
        final RelNode rel2 = RelOptMaterialization.toLeafJoinForm( queryRel );
        for ( PolyphenyDbSchema.TableEntry starTable : starTables ) {
            final Table table = starTable.getTable();
            assert table instanceof StarTable;
            RelOptTableImpl starRelOptTable = RelOptTableImpl.create( catalogReader, table.getRowType( typeFactory ), starTable, null );
            final RelNode rel3 = RelOptMaterialization.tryUseStar( rel2, starRelOptTable );
            if ( rel3 != null ) {
                list.add( new Callback( rel3, starTable, starRelOptTable ) );
            }
        }
        return list;
    }


    /**
     * Implementation of {@link RelShuttle} that returns each relational expression unchanged. It does not visit inputs.
     */
    static class RelNullShuttle implements RelShuttle {

        public RelNode visit( TableScan scan ) {
            return scan;
        }


        public RelNode visit( TableFunctionScan scan ) {
            return scan;
        }


        public RelNode visit( LogicalValues values ) {
            return values;
        }


        public RelNode visit( LogicalFilter filter ) {
            return filter;
        }


        public RelNode visit( LogicalProject project ) {
            return project;
        }


        public RelNode visit( LogicalJoin join ) {
            return join;
        }


        public RelNode visit( LogicalCorrelate correlate ) {
            return correlate;
        }


        public RelNode visit( LogicalUnion union ) {
            return union;
        }


        public RelNode visit( LogicalIntersect intersect ) {
            return intersect;
        }


        public RelNode visit( LogicalMinus minus ) {
            return minus;
        }


        public RelNode visit( LogicalAggregate aggregate ) {
            return aggregate;
        }


        public RelNode visit( LogicalMatch match ) {
            return match;
        }


        public RelNode visit( LogicalSort sort ) {
            return sort;
        }


        public RelNode visit( LogicalExchange exchange ) {
            return exchange;
        }


        public RelNode visit( RelNode other ) {
            return other;
        }
    }


    /**
     * Called when we discover a star table that matches.
     */
    static class Callback {

        public final RelNode rel;
        public final PolyphenyDbSchema.TableEntry starTable;
        public final RelOptTableImpl starRelOptTable;


        Callback( RelNode rel, PolyphenyDbSchema.TableEntry starTable, RelOptTableImpl starRelOptTable ) {
            this.rel = rel;
            this.starTable = starTable;
            this.starRelOptTable = starRelOptTable;
        }
    }
}


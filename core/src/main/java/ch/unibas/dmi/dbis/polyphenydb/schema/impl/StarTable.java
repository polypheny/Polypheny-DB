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

package ch.unibas.dmi.dbis.polyphenydb.schema.impl;


import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Virtual table that is composed of two or more tables joined together.
 *
 * Star tables do not occur in end-user queries. They are introduced by the optimizer to help matching queries to materializations, and used only during the planning process.
 *
 * When a materialization is defined, if it involves a join, it is converted to a query on top of a star table. Queries that are candidates to map onto the materialization are mapped onto the same star table.
 */
public class StarTable extends AbstractTable implements TranslatableTable {

    public final Lattice lattice;

    // TODO: we'll also need a list of join conditions between tables. For now we assume that join conditions match
    public final ImmutableList<Table> tables;

    /**
     * Number of fields in each table's row type.
     */
    public ImmutableIntList fieldCounts;


    /**
     * Creates a StarTable.
     */
    private StarTable( Lattice lattice, ImmutableList<Table> tables ) {
        this.lattice = Objects.requireNonNull( lattice );
        this.tables = tables;
    }


    /**
     * Creates a StarTable and registers it in a schema.
     */
    public static StarTable of( Lattice lattice, List<Table> tables ) {
        return new StarTable( lattice, ImmutableList.copyOf( tables ) );
    }


    @Override
    public TableType getJdbcTableType() {
        return Schema.TableType.STAR;
    }


    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final List<RelDataType> typeList = new ArrayList<>();
        final List<Integer> fieldCounts = new ArrayList<>();
        for ( Table table : tables ) {
            final RelDataType rowType = table.getRowType( typeFactory );
            typeList.addAll( RelOptUtil.getFieldTypeList( rowType ) );
            fieldCounts.add( rowType.getFieldCount() );
        }
        // Compute fieldCounts the first time this method is called. Safe to assume that the field counts will be the same whichever type factory is used.
        if ( this.fieldCounts == null ) {
            this.fieldCounts = ImmutableIntList.copyOf( fieldCounts );
        }
        return typeFactory.createStructType( typeList, lattice.uniqueColumnNames() );
    }


    public RelNode toRel( RelOptTable.ToRelContext context, RelOptTable table ) {
        // Create a table scan of infinite cost.
        return new StarTableScan( context.getCluster(), table );
    }


    public StarTable add( Table table ) {
        return of( lattice, ImmutableList.<Table>builder().addAll( tables ).add( table ).build() );
    }


    /**
     * Returns the column offset of the first column of {@code table} in this star table's output row type.
     *
     * @param table Table
     * @return Column offset
     * @throws IllegalArgumentException if table is not in this star
     */
    public int columnOffset( Table table ) {
        int n = 0;
        for ( Pair<Table, Integer> pair : Pair.zip( tables, fieldCounts ) ) {
            if ( pair.left == table ) {
                return n;
            }
            n += pair.right;
        }
        throw new IllegalArgumentException( "star table " + this + " does not contain table " + table );
    }


    /**
     * Relational expression that scans a {@link StarTable}.
     *
     * It has infinite cost.
     */
    public static class StarTableScan extends TableScan {

        public StarTableScan( RelOptCluster cluster, RelOptTable relOptTable ) {
            super( cluster, cluster.traitSetOf( Convention.NONE ), relOptTable );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeInfiniteCost();
        }
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * A <code>LogicalTableScan</code> reads all the rows from a {@link RelOptTable}.
 *
 * If the table is a <code>net.sf.saffron.ext.JdbcTable</code>, then this is literally possible. But for other kinds of tables, there may be many ways to read the data from the table.
 * For some kinds of table, it may not even be possible to read all of the rows unless some narrowing constraint is applied.
 *
 * In the example of the <code>net.sf.saffron.ext.ReflectSchema</code> schema,
 *
 * <blockquote>
 * <pre>select from fields</pre>
 * </blockquote>
 *
 * cannot be implemented, but
 *
 * <blockquote>
 * <pre>select from fields as f where f.getClass().getName().equals("java.lang.String")</pre>
 * </blockquote>
 *
 * can. It is the optimizer's responsibility to find these ways, by applying transformation rules.
 */
public final class LogicalTableScan extends TableScan {

    /**
     * Creates a LogicalTableScan.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table ) {
        super( cluster, traitSet, table );
    }


    /**
     * Creates a LogicalTableScan by parsing serialized output.
     */
    public LogicalTableScan( RelInput input ) {
        super( input );
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return this;
    }


    /**
     * Creates a LogicalTableScan.
     *
     * @param cluster Cluster
     * @param relOptTable Table
     */
    public static LogicalTableScan create( RelOptCluster cluster, final RelOptTable relOptTable ) {
        final Table table = relOptTable.unwrap( Table.class );
        final RelTraitSet traitSet =
                cluster.traitSetOf( Convention.NONE )
                        .replaceIfs( RelCollationTraitDef.INSTANCE, () -> {
                            if ( table != null ) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        } );
        return new LogicalTableScan( cluster, traitSet, relOptTable );
    }
}


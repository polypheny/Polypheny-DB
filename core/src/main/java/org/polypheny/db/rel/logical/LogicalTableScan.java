/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.rel.logical;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.schema.Table;


/**
 * A <code>LogicalTableScan</code> reads all the rows from a {@link RelOptTable}.
 *
 * If the table is a <code>net.sf.saffron.ext.JdbcTable</code>, then this is literally possible. But for other kinds of tables,
 * there may be many ways to read the data from the table. For some kinds of table, it may not even be possible to read all of
 * the rows unless some narrowing constraint is applied.
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


    @Override
    public void tryExpandView( RelNode input ) {
        // do nothing
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
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> {
                                    if ( table != null ) {
                                        return table.getStatistic().getCollations();
                                    }
                                    return ImmutableList.of();
                                } );

        return new LogicalTableScan( cluster, traitSet, relOptTable );
    }

}


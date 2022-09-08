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

package org.polypheny.db.algebra.logical.relational;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.Table;


/**
 * A <code>LogicalScan</code> reads all the rows from a {@link AlgOptTable}.
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
public final class LogicalScan extends Scan {


    /**
     * Creates a LogicalScan.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalScan( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table ) {
        super( cluster, traitSet, table );
    }


    /**
     * Creates a LogicalScan by parsing serialized output.
     */
    public LogicalScan( AlgInput input ) {
        super( input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return this;
    }


    /**
     * Creates a LogicalScan.
     *
     * @param cluster Cluster
     * @param algOptTable Table
     */
    public static LogicalScan create( AlgOptCluster cluster, final AlgOptTable algOptTable ) {
        final Table table = algOptTable.unwrap( Table.class );

        final AlgTraitSet traitSet =
                cluster.traitSetOf( Convention.NONE )
                        .replace( ModelTrait.RELATIONAL )
                        .replaceIfs(
                                AlgCollationTraitDef.INSTANCE,
                                () -> {
                                    if ( table != null ) {
                                        return table.getStatistic().getCollations();
                                    }
                                    return ImmutableList.of();
                                } );

        return new LogicalScan( cluster, traitSet, algOptTable );
    }

}


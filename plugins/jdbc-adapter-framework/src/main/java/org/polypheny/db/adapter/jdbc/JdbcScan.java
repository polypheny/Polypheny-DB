/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.jdbc.rel2sql.SqlImplementor.Result;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;


/**
 * Relational expression representing a relScan of a table in a JDBC data source.
 */
public class JdbcScan extends RelScan<JdbcTable> implements JdbcAlg {

    protected final JdbcTable jdbcTable;


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.9 );
    }


    public JdbcScan( AlgCluster cluster, JdbcTable jdbcTable, JdbcConvention jdbcConvention ) {
        super( cluster, cluster.traitSetOf( jdbcConvention ).replace( ModelTrait.RELATIONAL ), jdbcTable );
        this.jdbcTable = jdbcTable;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new JdbcScan( getCluster(), entity, (JdbcConvention) getConvention() );
    }


    @Override
    public Result implement( JdbcImplementor implementor ) {
        return implementor.result(
                jdbcTable.physicalTableName(),
                ImmutableList.of( JdbcImplementor.Clause.FROM ),
                this,
                null );
    }

}


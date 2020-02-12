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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.rel2sql.SqlImplementor.Result;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Relational expression representing a scan of a table in a JDBC data source.
 */
public class JdbcTableScan extends TableScan implements JdbcRel {

    protected final JdbcTable jdbcTable;


    protected JdbcTableScan( RelOptCluster cluster, RelOptTable table, JdbcTable jdbcTable, JdbcConvention jdbcConvention ) {
        super( cluster, cluster.traitSetOf( jdbcConvention ), table );
        this.jdbcTable = jdbcTable;
        assert jdbcTable != null;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert inputs.isEmpty();
        return new JdbcTableScan( getCluster(), table, jdbcTable, (JdbcConvention) getConvention() );
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


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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraRel.CassandraImplementContext.Type;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Relational expression representing a scan of a Cassandra collection.
 */
public class CassandraTableScan extends TableScan implements CassandraRel {

    public final CassandraTable cassandraTable;
    final RelDataType projectRowType;


    /**
     * Creates a CassandraTableScan.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table Table
     * @param cassandraTable Cassandra table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected CassandraTableScan( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CassandraTable cassandraTable, RelDataType projectRowType ) {
        super( cluster, traitSet, table );
        this.cassandraTable = cassandraTable;
        this.projectRowType = projectRowType;

        assert cassandraTable != null;
        // TODO JS: Check this
//        assert getConvention() == CONVENTION;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert inputs.isEmpty();
        return new CassandraTableScan( getCluster(), traitSet, this.table, this.cassandraTable, this.projectRowType );
    }


    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }


    @Override
    public void register( RelOptPlanner planner ) {
        // TODO JS: Double check
//        planner.addRule( CassandraToEnumerableConverterRule.INSTANCE );
        getConvention().register( planner );
//        for ( RelOptRule rule : getConvention() ) {
//            planner.addRule( rule );
//        }
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( CassandraConvention.COST_MULTIPLIER );
    }


    @Override
    public void implement( CassandraImplementContext context ) {



        context.cassandraTable = cassandraTable;
        context.table = table;

        if ( context.type != null ) {
            return;
        }

        context.type = Type.SELECT;

        SelectFrom selectFrom = QueryBuilder.selectFrom( context.cassandraTable.getColumnFamily() );

//        final RelProtoDataType resultRowType = RelDataTypeImpl.proto( fieldInfo.build() );

        Select select;
        // Construct the list of fields to project
        if ( context.selectFields.isEmpty() ) {
            select = selectFrom.all();
        } else {
            select = selectFrom.selectors( context.selectFields );
        }

        select = select.where( context.whereClause );

        // FIXME js: Horrible hack, but hopefully works for now till I understand everything better.
        Map<String, ClusteringOrder> orderMap = new LinkedHashMap<>();
        for (Map.Entry<String, ClusteringOrder> entry: context.order.entrySet() ) {
            orderMap.put( entry.getKey(), entry.getValue() );
        }

        select = select.orderBy( orderMap );
        int limit = context.offset;
        if ( context.fetch >= 0 ) {
            limit += context.fetch;
        }
        if ( limit > 0 ) {
            select = select.limit( limit );
        }

        select = select.allowFiltering();


        final SimpleStatement statement = select.build();

//        implementor.simpleStatement = statement;
    }
}


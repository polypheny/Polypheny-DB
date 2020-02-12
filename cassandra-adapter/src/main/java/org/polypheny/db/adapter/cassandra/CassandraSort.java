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

package org.polypheny.db.adapter.cassandra;


import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexNode;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Implementation of {@link Sort} relational expression in Cassandra.
 */
public class CassandraSort extends Sort implements CassandraRel {

    public CassandraSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, child, collation, offset, fetch );

        // TODO JS: Check this
//        assert getConvention() == CONVENTION;
//        assert getConvention() == child.getConvention();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        RelOptCost cost = super.computeSelfCost( planner, mq );
        if ( !collation.getFieldCollations().isEmpty() ) {
            return cost.multiplyBy( 0.05 );
        } else {
            return cost;
        }
    }


    @Override
    public Sort copy( RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch ) {
        return new CassandraSort( getCluster(), traitSet, input, collation, offset, fetch );
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        context.visitChild( 0, getInput() );

        List<RelFieldCollation> sortCollations = collation.getFieldCollations();
        Map<String, ClusteringOrder> fieldOrder = new LinkedHashMap<>();
        if ( !sortCollations.isEmpty() ) {
            // Construct a series of order clauses from the desired collation
            final List<RelDataTypeField> fields = getRowType().getFieldList();
            for ( RelFieldCollation fieldCollation : sortCollations ) {
                final String name =
                        fields.get( fieldCollation.getFieldIndex() ).getName();
                final ClusteringOrder direction;
                switch ( fieldCollation.getDirection() ) {
                    case DESCENDING:
                        direction = ClusteringOrder.DESC;
                        break;
                    default:
                        direction = ClusteringOrder.ASC;
                }
                fieldOrder.put( name, direction );
            }

            context.addOrder( fieldOrder );
        }
    }
}


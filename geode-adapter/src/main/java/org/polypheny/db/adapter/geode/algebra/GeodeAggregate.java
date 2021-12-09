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

package org.polypheny.db.adapter.geode.algebra;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Aggregate} relational expression in Geode.
 */
public class GeodeAggregate extends Aggregate implements GeodeAlg {

    /**
     * Creates a GeodeAggregate.
     */
    public GeodeAggregate( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );

        assert getConvention() == GeodeAlg.CONVENTION;
        assert getConvention() == this.input.getConvention();
        assert getConvention() == input.getConvention();
        assert this.groupSets.size() == 1 : "Grouping sets not supported";

        for ( AggregateCall aggCall : aggCalls ) {
            if ( aggCall.isDistinct() ) {
                System.out.println( "DISTINCT based aggregation!" );
            }
        }
    }


    @Override
    public Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return new GeodeAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( GeodeImplementContext geodeImplementContext ) {
        geodeImplementContext.visitChild( getInput() );

        List<String> inputFields = fieldNames( getInput().getRowType() );
        List<String> groupByFields = new ArrayList<>();

        for ( int group : groupSet ) {
            groupByFields.add( inputFields.get( group ) );
        }

        geodeImplementContext.addGroupBy( groupByFields );

        // Find the aggregate functions (e.g. MAX, SUM ...)
        Builder<String, String> aggregateFunctionMap = ImmutableMap.builder();
        for ( AggregateCall aggCall : aggCalls ) {

            List<String> aggCallFieldNames = new ArrayList<>();
            for ( int i : aggCall.getArgList() ) {
                aggCallFieldNames.add( inputFields.get( i ) );
            }
            String functionName = aggCall.getAggregation().getName();

            // Workaround to handle count(*) case. Geode doesn't allow "AS" aliases on 'count(*)' but allows it for count('any column name'). So we are converting the count(*) into count (first input ColumnName).
            if ( "COUNT".equalsIgnoreCase( functionName ) && aggCallFieldNames.isEmpty() ) {
                aggCallFieldNames.add( inputFields.get( 0 ) );
            }

            String oqlAggregateCall = Util.toString( aggCallFieldNames, functionName + "(", ", ", ")" );

            aggregateFunctionMap.put( aggCall.getName(), oqlAggregateCall );
        }

        geodeImplementContext.addAggregateFunctions( aggregateFunctionMap.build() );

    }


    private List<String> fieldNames( AlgDataType algDataType ) {
        ArrayList<String> names = new ArrayList<>();

        for ( AlgDataTypeField rdtf : algDataType.getFieldList() ) {
            names.add( rdtf.getName() );
        }
        return names;
    }

}

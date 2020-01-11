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

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate} relational expression in Geode.
 */
public class GeodeAggregate extends Aggregate implements GeodeRel {

    /**
     * Creates a GeodeAggregate.
     */
    public GeodeAggregate( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );

        assert getConvention() == GeodeRel.CONVENTION;
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
    public Aggregate copy( RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return new GeodeAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
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


    private List<String> fieldNames( RelDataType relDataType ) {
        ArrayList<String> names = new ArrayList<>();

        for ( RelDataTypeField rdtf : relDataType.getFieldList() ) {
            names.add( rdtf.getName() );
        }
        return names;
    }
}

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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.InvalidRelException;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;


/**
 * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate} relational expression for ElasticSearch.
 */
public class ElasticsearchAggregate extends Aggregate implements ElasticsearchRel {

    private static final Set<SqlKind> SUPPORTED_AGGREGATIONS = EnumSet.of( SqlKind.COUNT, SqlKind.MAX, SqlKind.MIN, SqlKind.AVG, SqlKind.SUM, SqlKind.ANY_VALUE );


    /**
     * Creates a ElasticsearchAggregate
     */
    ElasticsearchAggregate( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) throws InvalidRelException {
        super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );

        if ( getConvention() != input.getConvention() ) {
            String message = String.format( Locale.ROOT, "%s != %s", getConvention(), input.getConvention() );
            throw new AssertionError( message );
        }

        assert getConvention() == input.getConvention();
        assert getConvention() == CONVENTION;
        assert this.groupSets.size() == 1 : "Grouping sets not supported";

        for ( AggregateCall aggCall : aggCalls ) {
            if ( aggCall.isDistinct() && !aggCall.isApproximate() ) {
                final String message = String.format( Locale.ROOT, "Only approximate distinct aggregations are supported in Elastic (cardinality aggregation). Use %s function", SqlStdOperatorTable.APPROX_COUNT_DISTINCT.getName() );
                throw new InvalidRelException( message );
            }

            final SqlKind kind = aggCall.getAggregation().getKind();
            if ( !SUPPORTED_AGGREGATIONS.contains( kind ) ) {
                final String message = String.format( Locale.ROOT, "Aggregation %s not supported (use one of %s)", kind, SUPPORTED_AGGREGATIONS );
                throw new InvalidRelException( message );
            }
        }

        if ( getGroupType() != Group.SIMPLE ) {
            final String message = String.format( Locale.ROOT, "Only %s grouping is supported. Yours is %s", Group.SIMPLE, getGroupType() );
            throw new InvalidRelException( message );
        }

    }


    @Override
    public Aggregate copy( RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        try {
            return new ElasticsearchAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
        } catch ( InvalidRelException e ) {
            throw new AssertionError( e );
        }
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        final List<String> inputFields = fieldNames( getInput().getRowType() );
        for ( int group : groupSet ) {
            final String name = inputFields.get( group );
            implementor.addGroupBy( implementor.expressionItemMap.getOrDefault( name, name ) );
        }

        final ObjectMapper mapper = implementor.elasticsearchTable.mapper;

        for ( AggregateCall aggCall : aggCalls ) {
            final List<String> names = new ArrayList<>();
            for ( int i : aggCall.getArgList() ) {
                names.add( inputFields.get( i ) );
            }

            final ObjectNode aggregation = mapper.createObjectNode();
            final ObjectNode field = aggregation.with( toElasticAggregate( aggCall ) );

            final String name = names.isEmpty() ? ElasticsearchConstants.ID : names.get( 0 );
            field.put( "field", implementor.expressionItemMap.getOrDefault( name, name ) );
            if ( aggCall.getAggregation().getKind() == SqlKind.ANY_VALUE ) {
                field.put( "size", 1 );
            }

            implementor.addAggregation( aggCall.getName(), aggregation.toString() );
        }
    }


    /**
     * Most of the aggregations can be retrieved with single <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html">stats</a> function.
     * But currently only one-to-one mapping is supported between sql agg and elastic aggregation.
     */
    private static String toElasticAggregate( AggregateCall call ) {
        final SqlKind kind = call.getAggregation().getKind();
        switch ( kind ) {
            case COUNT:
                // approx_count_distinct() vs count()
                return call.isDistinct() && call.isApproximate() ? "cardinality" : "value_count";
            case SUM:
                return "sum";
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
            case ANY_VALUE:
                return "terms";
            default:
                throw new IllegalArgumentException( "Unknown aggregation kind " + kind + " for " + call );
        }
    }


    private List<String> fieldNames( RelDataType relDataType ) {
        List<String> names = new ArrayList<>();

        for ( RelDataTypeField rdtf : relDataType.getFieldList() ) {
            names.add( rdtf.getName() );
        }
        return names;
    }

}

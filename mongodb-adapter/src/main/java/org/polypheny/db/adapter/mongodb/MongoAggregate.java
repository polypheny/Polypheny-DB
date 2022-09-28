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

package org.polypheny.db.adapter.mongodb;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.sql.language.fun.SqlSingleValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Aggregate} relational expression in MongoDB.
 */
public class MongoAggregate extends Aggregate implements MongoAlg {

    public MongoAggregate( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) throws InvalidAlgException {
        super( cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();

        for ( AggregateCall aggCall : aggCalls ) {
            if ( aggCall.isDistinct() ) {
                throw new InvalidAlgException( "distinct aggregation not supported" );
            }
        }
        switch ( getGroupType() ) {
            case SIMPLE:
                break;
            default:
                throw new InvalidAlgException( "unsupported group type: " + getGroupType() );
        }
    }


    @Override
    public Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        try {
            return new MongoAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        List<String> list = new ArrayList<>();
        final List<String> inNames = MongoRules.mongoFieldNames( getInput().getRowType() );
        final List<String> outNames = MongoRules.mongoFieldNames( getRowType() );
        int i = 0;
        if ( groupSet.cardinality() == 1 ) {
            final String inName = inNames.get( groupSet.nth( 0 ) );
            list.add( "_id: " + MongoRules.maybeQuote( "$" + inName ) );
            implementor.physicalMapper.add( inName );
            ++i;
        } else {
            List<String> keys = new ArrayList<>();
            for ( int group : groupSet ) {
                final String inName = inNames.get( group );
                keys.add( inName + ": " + MongoRules.quote( "$" + inName ) );
                implementor.physicalMapper.add( inName );
                ++i;
            }
            list.add( "_id: " + Util.toString( keys, "{", ", ", "}" ) );
        }
        for ( AggregateCall aggCall : aggCalls ) {
            list.add( MongoRules.maybeQuote( outNames.get( i++ ) ) + ": " + toMongo( aggCall.getAggregation(), inNames, aggCall.getArgList(), implementor ) );
        }
        implementor.add( null, "{$group: " + Util.toString( list, "{", ", ", "}" ) + "}" );
        final List<String> fixups;
        if ( groupSet.cardinality() == 1 ) {
            fixups = new AbstractList<>() {
                @Override
                public String get( int index ) {
                    final String outName = outNames.get( index );
                    return MongoRules.maybeQuote( outName ) + ": " + MongoRules.maybeQuote( "$" + (index == 0 ? "_id" : outName) );
                }


                @Override
                public int size() {
                    return outNames.size();
                }
            };
        } else {
            fixups = new ArrayList<>();
            fixups.add( "_id: 0" );
            i = 0;
            for ( int group : groupSet ) {
                fixups.add( MongoRules.maybeQuote( outNames.get( group ) ) + ": " + MongoRules.maybeQuote( "$_id." + outNames.get( group ) ) );
                ++i;
            }
            for ( AggregateCall ignored : aggCalls ) {
                final String outName = outNames.get( i++ );
                fixups.add( MongoRules.maybeQuote( outName ) + ": " + MongoRules.maybeQuote( "$" + outName ) );
            }
        }
        if ( !groupSet.isEmpty() ) {
            implementor.add( null, "{$project: " + Util.toString( fixups, "{", ", ", "}" ) + "}" );
        }
    }


    private String toMongo( AggFunction aggregation, List<String> inNames, List<Integer> args, Implementor implementor ) {
        if ( aggregation.getOperatorName() == OperatorName.COUNT ) {
            if ( args.size() == 0 ) {
                return "{$sum: 1}";
            } else {
                assert args.size() == 1;
                final String inName = inNames.get( args.get( 0 ) );
                implementor.physicalMapper.add( inName );
                return "{$sum: {$cond: [ {$eq: [" + MongoRules.quote( inName ) + ", null]}, 0, 1]}}";
            }
        } else if ( aggregation instanceof SqlSumAggFunction || aggregation instanceof SqlSumEmptyIsZeroAggFunction ) {
            assert args.size() == 1;
            final String inName = inNames.get( args.get( 0 ) );
            implementor.physicalMapper.add( inName );
            return "{$sum: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggregation.getOperatorName() == OperatorName.MIN ) {
            assert args.size() == 1;
            final String inName = inNames.get( args.get( 0 ) );
            implementor.physicalMapper.add( inName );
            return "{$min: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggregation.equals( OperatorRegistry.getAgg( OperatorName.MAX ) ) ) {
            assert args.size() == 1;
            final String inName = inNames.get( args.get( 0 ) );
            implementor.physicalMapper.add( inName );
            return "{$max: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggregation.getOperatorName() == OperatorName.AVG || aggregation.getKind() == OperatorRegistry.getAgg( OperatorName.AVG ).getKind() ) {
            assert args.size() == 1;
            final String inName = inNames.get( args.get( 0 ) );
            implementor.physicalMapper.add( inName );
            return "{$avg: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggregation instanceof SqlSingleValueAggFunction ) {
            assert args.size() == 1;
            final String inName = inNames.get( args.get( 0 ) );
            implementor.physicalMapper.add( inName );
            return "{$sum:" + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else {
            throw new AssertionError( "unknown aggregate " + aggregation );
        }
    }

}


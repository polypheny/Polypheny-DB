/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.adapter.mongodb.rules;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.sql.language.fun.SqlSingleValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.util.Util;

public class MongoDocumentAggregate extends DocumentAggregate implements MongoAlg {

    /**
     * Creates a {@link DocumentAggregate}.
     * {@link ModelTrait#DOCUMENT} native node of an aggregate.
     *
     * @param cluster
     * @param traits
     * @param child
     * @param indicator
     * @param groupSet
     * @param groupSets
     * @param aggCalls
     * @param names
     */
    protected MongoDocumentAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, @NotNull List<String> groupSet, List<List<String>> groupSets, List<AggregateCall> aggCalls, List<String> names ) {
        super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls, names );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        List<String> list = new ArrayList<>();
        final List<String> inNames = MongoRules.mongoFieldNames( getInput().getRowType() );
        final List<String> outNames = MongoRules.mongoFieldNames( getRowType() );
        int i = 0;

        final String inName = groupSet.get( 0 );
        list.add( "_id: " + MongoRules.maybeQuote( "$" + inName ) );
        implementor.physicalMapper.add( inName );
        ++i;

        for ( AggregateCall aggCall : aggCalls ) {
            list.add( MongoRules.maybeQuote( outNames.get( i++ ) ) + ": " + toMongo( aggCall.getAggregation(), inNames, aggCall.getArgList(), implementor ) );
        }
        implementor.add( null, "{$group: " + Util.toString( list, "{", ", ", "}" ) + "}" );
        final List<String> fixups;

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

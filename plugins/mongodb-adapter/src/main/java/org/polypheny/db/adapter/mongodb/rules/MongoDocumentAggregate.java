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
 */

package org.polypheny.db.adapter.mongodb.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNameRef;
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
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this expression
     * @param child Input of this expression
     * @param aggCalls Aggregate calls
     */
    protected MongoDocumentAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, RexNameRef group, List<LaxAggregateCall> aggCalls ) {
        super( cluster, traits, child, group, aggCalls );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        List<String> list = new ArrayList<>();

        if ( getGroup().isEmpty() && aggCalls.size() == 1 ) {
            handleSpecificAggregate( implementor, list, aggCalls.get( 0 ) );
            return;
        }

        final String inName = MongoRules.maybeQuote( getGroup().map( n -> "$" + n.unwrap( RexNameRef.class ).orElseThrow().name ).orElseThrow() );

        list.add( "_id: " + inName );
        //implementor.physicalMapper.add( inName );

        for ( LaxAggregateCall aggCall : aggCalls ) {
            list.add( MongoRules.maybeQuote( aggCall.name ) + ": " + toMongo( aggCall ) );
        }
        implementor.add( null, "{$group: " + Util.toString( list, "{", ", ", "}" ) + "}" );
    }


    private void handleSpecificAggregate( Implementor implementor, List<String> list, LaxAggregateCall call ) {

        if ( Objects.requireNonNull( call.function.getKind() ) == Kind.COUNT ) {
            implementor.add( null, "{$count: \"" + call.name + "\" }" );
        } else {
            throw new GenericRuntimeException( "unknown aggregate " + call.function );
        }

    }


    private String toMongo( LaxAggregateCall aggCall ) {
        if ( aggCall.function.getOperatorName() == OperatorName.COUNT ) {
            if ( aggCall.getInput().isEmpty() ) {
                return "{$sum: 1}";
            } else {
                assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
                final String inName = ((RexNameRef) aggCall.getInput().get()).name;
                return "{$sum: {$cond: [ {$eq: [" + MongoRules.quote( inName ) + ", null]}, 0, 1]}}";
            }
        } else if ( aggCall.function instanceof SqlSumAggFunction || aggCall.function instanceof SqlSumEmptyIsZeroAggFunction ) {
            assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
            final String inName = ((RexNameRef) aggCall.getInput().get()).name;
            return "{$sum: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggCall.function.getOperatorName() == OperatorName.MIN ) {
            assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
            final String inName = ((RexNameRef) aggCall.getInput().get()).name;
            return "{$min: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggCall.function.equals( OperatorRegistry.getAgg( OperatorName.MAX ) ) ) {
            assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
            final String inName = ((RexNameRef) aggCall.getInput().get()).name;
            return "{$max: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggCall.function.getOperatorName() == OperatorName.AVG || aggCall.function.getKind() == OperatorRegistry.getAgg( OperatorName.AVG ).getKind() ) {
            assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
            final String inName = ((RexNameRef) aggCall.getInput().get()).name;
            return "{$avg: " + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else if ( aggCall.function instanceof SqlSingleValueAggFunction ) {
            assert aggCall.getInput().get().unwrap( RexNameRef.class ).orElseThrow().getNames().size() == 1;
            final String inName = ((RexNameRef) aggCall.getInput().get()).name;
            return "{$sum:" + MongoRules.maybeQuote( "$" + inName ) + "}";
        } else {
            throw new GenericRuntimeException( "unknown aggregate " + aggCall.function );
        }
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoDocumentAggregate( getCluster(), traitSet, sole( inputs ), getGroup().orElse( null ), aggCalls );
    }

}

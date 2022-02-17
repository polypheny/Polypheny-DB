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

package org.polypheny.db.adapter.mongodb;


import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Project} relational expression in MongoDB.
 */
public class MongoProject extends Project implements MongoAlg {

    public MongoProject( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() == CONVENTION;
        //assert getConvention() == input.getConvention(); // TODO DL fix logicalFilter bug
    }


    @Override
    public Project copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new MongoProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );

        final MongoRules.RexToMongoTranslator translator = new MongoRules.RexToMongoTranslator( (JavaTypeFactory) getCluster().getTypeFactory(), MongoRules.mongoFieldNames( getInput().getRowType() ), implementor );
        final List<String> items = new ArrayList<>();
        final List<String> excludes = new ArrayList<>();
        final List<String> unwinds = new ArrayList<>();
        // we us our specialized rowType to derive the mapped underlying column identifiers
        MongoRowType mongoRowType = null;
        if ( implementor.getStaticRowType() instanceof MongoRowType ) {
            mongoRowType = ((MongoRowType) implementor.getStaticRowType());
        }

        BsonDocument documents = new BsonDocument();

        if ( getNamedProjects().size() > 1 && getNamedProjects().get( 0 ).right.contains( "." ) && implementor.hasProject ) {
            // we already cast so we can skip this whole iteration
            return;
        }

        for ( Pair<RexNode, String> pair : getNamedProjects() ) {
            final String name = MongoRules.adjustName( pair.right );

            if ( pair.left.getKind() == Kind.DISTANCE ) {
                documents.put( name, BsonFunctionHelper.getFunction( (RexCall) pair.left, mongoRowType, implementor ) );
                continue;
            }

            if ( pair.left instanceof RexCall ) {
                if ( ((RexCall) pair.left).operands.get( 0 ).isA( Kind.MQL_ADD_FIELDS ) ) {
                    Pair<String, RexNode> ret = MongoRules.getAddFields( (RexCall) ((RexCall) pair.left).operands.get( 0 ), rowType );
                    String expr = ret.right.accept( translator );
                    implementor.preProjections.add( new BsonDocument( ret.left, BsonDocument.parse( expr ) ) );
                    items.add( ret.left.split( "\\." )[0] + ":1" );
                    continue;
                }
            }

            String expr = pair.left.accept( translator );
            if ( expr == null ) {
                continue;
            }

            // exclude projection cannot be handled this way, so it needs fixing
            KindChecker visitor = new KindChecker( Kind.MQL_EXCLUDE );
            pair.left.accept( visitor );
            if ( visitor.containsKind ) {
                items.add( name + ":1" );
                excludes.add( expr );
                continue;
            }

            visitor = new KindChecker( Kind.UNWIND );
            pair.left.accept( visitor );
            if ( visitor.containsKind ) {
                // $unwinds need to projected out else $unwind is not possible
                items.add( name + ":" + expr );
                unwinds.add( "\"$" + name + "\"" );
                continue;
            }

            items.add( expr.equals( "'$" + name + "'" )
                    ? MongoRules.maybeQuote( name ) + ": " + 1
                    : MongoRules.maybeQuote( name ) + ": " + expr );
        }
        List<String> mergedItems;

        if ( documents.size() != 0 ) {
            String functions = documents.toJson( JsonWriterSettings.builder().outputMode( JsonMode.RELAXED ).build() );
            mergedItems = Streams.concat( items.stream(), Stream.of( functions.substring( 1, functions.length() - 1 ) ) )
                    .collect( Collectors.toList() );
        } else {
            mergedItems = items;
        }

        String findString = Util.toString(
                mergedItems,
                "{", ", ", "}" );
        final String aggregateString = "{$project: " + findString + "}";
        final Pair<String, String> op = Pair.of( findString, aggregateString );

        implementor.hasProject = true;
        if ( !implementor.isDML() && items.size() + documents.size() != 0 ) {
            implementor.add( op.left, op.right );
            if ( unwinds.size() != 0 ) {
                implementor.add( Util.toString( unwinds, "{", ",", "}" ), Util.toString( unwinds, "{$unwind:", ",", "}" ) );
            }
        }
        if ( excludes.size() != 0 ) {
            String excludeString = Util.toString(
                    excludes,
                    "{", ", ", "}" );
            implementor.add( excludeString, "{$project: " + excludeString + "}" );
        }
    }


    public static class KindChecker extends RexVisitorImpl<Void> {

        private final Kind kind;
        @Getter
        boolean containsKind = false;


        protected KindChecker( Kind kind ) {
            super( true );
            this.kind = kind;
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( call.isA( kind ) ) {
                containsKind = true;
                return null;
            } else {
                call.operands.forEach( node -> node.accept( this ) );
                return null;
            }
        }

    }

}


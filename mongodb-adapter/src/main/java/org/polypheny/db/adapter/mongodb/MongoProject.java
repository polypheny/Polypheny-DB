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

package org.polypheny.db.adapter.mongodb;


import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Project} relational expression in MongoDB.
 */
public class MongoProject extends Project implements MongoRel {

    public MongoProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() == CONVENTION;
        //assert getConvention() == input.getConvention(); // TODO DL: check
    }


    @Override
    public Project copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
        return new MongoProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );

        final MongoRules.RexToMongoTranslator translator = new MongoRules.RexToMongoTranslator( (JavaTypeFactory) getCluster().getTypeFactory(), MongoRules.mongoFieldNames( getInput().getRowType() ) );
        final List<String> items = new ArrayList<>();
        GridFSBucket bucket = implementor.getBucket();
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
            final String name = MongoRules.maybeFix( pair.right );
            String phyName = "1";

            if ( pair.left.getKind() == SqlKind.DISTANCE ) {
                documents.put( pair.right, BsonFunctionHelper.getFunction( (RexCall) pair.left, mongoRowType, bucket ) );
                continue;
            }

            String expr = pair.left.accept( translator );
            if ( expr == null ) {
                continue;
            }

            // we can you use a project of [name] : $[physical name] to rename our retrieved columns on aggregation
            // we have to pay attention to "DUMMY" as it is apparently used for handling aggregates here
            if ( mongoRowType != null && !name.contains( "$" ) && !name.equals( "DUMMY" ) ) {
                if ( mongoRowType.containsPhysicalName( MongoRules.maybeFix( name ) ) ) {
                    phyName = "\"$" + mongoRowType.getPhysicalName( MongoRules.maybeFix( name ) ) + "\"";
                }
            }

            StringBuilder blankExpr = new StringBuilder( expr.replace( "'", "" ) );
            if ( blankExpr.toString().startsWith( "$" ) && blankExpr.toString().endsWith( "]" ) && blankExpr.toString().contains( "[" ) ) {
                // we want to access an array element and have to get the correct table and the specified array element
                String[] splits = blankExpr.toString().split( "\\[" );
                if ( splits.length >= 2 && splits[1].contains( "]" ) && mongoRowType != null ) {
                    String arrayName = splits[0].replace( "$", "" );
                    arrayName = "\"$" + mongoRowType.getPhysicalName( arrayName ) + "\"";

                    // we can have multidimensional arrays and have to take care here
                    blankExpr = new StringBuilder( arrayName );
                    for ( int i = 1; i < splits.length; i++ ) {
                        if ( splits[i].startsWith( "{" ) && splits[i].endsWith( "}]" ) ) {
                            String dynamic = splits[i].substring( 0, splits[i].length() - 1 );
                            blankExpr = new StringBuilder( "{$arrayElemAt:[" + blankExpr + ", {$add: [" + dynamic + ", -1]}]}" );
                        } else {
                            // we have to adjust as sql arrays start at 1
                            int pos = Integer.parseInt( splits[i].replace( "]", "" ) ) - 1;
                            blankExpr = new StringBuilder( "{$arrayElemAt:[" + blankExpr + ", " + pos + "]}" );
                        }
                    }
                    expr = blankExpr.toString();

                }

            }

            items.add( expr.equals( "'$" + name + "'" )
                    ? MongoRules.maybeQuote( name ) + ": " + phyName//1"
                    : MongoRules.maybeQuote( name ) + ": " + expr );
        }
        String functions = documents.toJson( JsonWriterSettings.builder().outputMode( JsonMode.RELAXED ).build() );
        String findString = Util.toString( items, "{", ", ", "" ) + functions.substring( 1, functions.length() - 1 ) + "}";
        final String aggregateString = "{$project: " + findString + "}";
        final Pair<String, String> op = Pair.of( findString, aggregateString );
        implementor.hasProject = true;
        if ( !implementor.isDML() && items.size() + documents.size() != 0 ) {
            implementor.add( op.left, op.right );
        }
    }

}


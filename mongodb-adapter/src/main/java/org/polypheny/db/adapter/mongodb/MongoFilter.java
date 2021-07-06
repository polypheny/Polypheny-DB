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


import static org.polypheny.db.sql.SqlKind.DISTANCE;
import static org.polypheny.db.sql.SqlKind.DYNAMIC_PARAM;
import static org.polypheny.db.sql.SqlKind.INPUT_REF;
import static org.polypheny.db.sql.SqlKind.LITERAL;
import static org.polypheny.db.sql.SqlKind.OTHER_FUNCTION;

import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlItemOperator;
import org.polypheny.db.util.JsonBuilder;


/**
 * Implementation of a {@link org.polypheny.db.rel.core.Filter}
 * relational expression in MongoDB.
 */
public class MongoFilter extends Filter implements MongoRel {

    public MongoFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public MongoFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new MongoFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        // to not break the existing functionality for now we have to handle it this way
        Translator translator;
        if ( implementor.getStaticRowType() != null && implementor.getStaticRowType() instanceof MongoRowType ) {
            translator = new Translator( MongoRules.mongoFieldNames( getRowType() ), (MongoRowType) implementor.getStaticRowType(), implementor );
        } else {
            translator = new Translator( MongoRules.mongoFieldNames( getRowType() ), implementor );
        }
        translator.translateMatch( condition, implementor );
    }


    /**
     * Translates {@link RexNode} expressions into MongoDB expression strings.
     */
    static class Translator {

        final JsonBuilder builder = new JsonBuilder();
        private final List<String> fieldNames;
        private final MongoRowType rowType;
        private final List<BsonDocument> dynamics = new ArrayList<>();
        private final GridFSBucket bucket;
        private final BsonDocument preProjections = new BsonDocument();
        private final Implementor implementor;


        Translator( List<String> fieldNames, Implementor implementor ) {
            this( fieldNames, null, implementor );
        }


        Translator( List<String> fieldNames, MongoRowType rowType, Implementor implementor ) {
            this.builder.setMongo( true );
            this.fieldNames = fieldNames;
            this.rowType = rowType;
            this.bucket = implementor.bucket;
            this.implementor = implementor;
        }


        private void translateMatch( RexNode condition, Implementor implementor ) {
            BsonDocument value = translateOr( condition );
            if ( !value.isEmpty() ) {
                implementor.filter.add( value );
            }

            if ( preProjections.size() != 0 ) {
                implementor.preProjections.add( preProjections );
            }
        }


        private BsonDocument translateOr( RexNode condition ) {
            for ( RexNode node : RelOptUtil.disjunctions( condition ) ) {
                translateAnd( node );
            }

            switch ( dynamics.size() ) {
                case 0:
                    return new BsonDocument();
                case 1:
                    return dynamics.get( 0 );
                default:
                    return new BsonDocument( "$or", new BsonArray( dynamics ) );
            }
        }


        /**
         * Translates a condition that may be an AND of other conditions. Gathers together conditions that apply to the same field.
         */
        private void translateAnd( RexNode node0 ) {
            for ( RexNode node : RelOptUtil.conjunctions( node0 ) ) {
                translateMatch2( node );
            }
        }


        private void addPredicate( Map<String, Object> map, String op, Object v ) {
            if ( map.containsKey( op ) && stronger( op, map.get( op ), v ) ) {
                return;
            }
            map.put( op, v );
        }


        /**
         * Returns whether {@code v0} is a stronger value for operator {@code key} than {@code v1}.
         *
         * For example, {@code stronger("$lt", 100, 200)} returns true, because "&lt; 100" is a more powerful condition than "&lt; 200".
         */
        private boolean stronger( String key, Object v0, Object v1 ) {
            if ( key.equals( "$lt" ) || key.equals( "$lte" ) ) {
                if ( v0 instanceof Number && v1 instanceof Number ) {
                    return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
                }
                if ( v0 instanceof String && v1 instanceof String ) {
                    return v0.toString().compareTo( v1.toString() ) < 0;
                }
            }
            if ( key.equals( "$gt" ) || key.equals( "$gte" ) ) {
                return stronger( "$lt", v1, v0 );
            }
            return false;
        }


        private Void translateMatch2( RexNode node ) {
            switch ( node.getKind() ) {
                case EQUALS:
                    return translateBinary( null, null, (RexCall) node );
                case LESS_THAN:
                    return translateBinary( "$lt", "$gt", (RexCall) node );
                case LESS_THAN_OR_EQUAL:
                    return translateBinary( "$lte", "$gte", (RexCall) node );
                case NOT_EQUALS:
                    return translateBinary( "$ne", "$ne", (RexCall) node );
                case GREATER_THAN:
                    return translateBinary( "$gt", "$lt", (RexCall) node );
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary( "$gte", "$lte", (RexCall) node );
                case LIKE:
                    return translateLike( (RexCall) node );
                default:
                    throw new AssertionError( "cannot translate " + node );
            }
        }


        private Void translateLike( RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );

            switch ( right.getKind() ) {
                case DYNAMIC_PARAM:
                    this.dynamics.add(
                            new BsonDocument()
                                    .append(
                                            getPhysicalName( (RexInputRef) left ),
                                            new BsonDynamic( (RexDynamicParam) right ).setIsRegex( true ) ) );
                    return null;
                case LITERAL:

                    this.dynamics.add( new BsonDocument(
                            getPhysicalName( (RexInputRef) left ), MongoTypeUtil.replaceLikeWithRegex( ((RexLiteral) right).getValueAs( String.class ) ) ) );
                    return null;
                default:
                    throw new IllegalStateException( "Unexpected value: " + right.getKind() );
            }
        }


        /**
         * Translates a call to a binary operator, reversing arguments if necessary.
         */
        private Void translateBinary( String op, String rop, RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            boolean b = translateBinary2( op, left, right );
            if ( b ) {
                return null;
            }
            b = translateBinary2( rop, right, left );
            if ( b ) {
                return null;
            }
            b = translateArray( op, right, left );
            if ( b ) {
                return null;
            }
            b = translateArray( op, left, right );
            if ( b ) {
                return null;
            }

            b = translateExpr( op, left, right );
            if ( b ) {
                return null;
            }

            throw new AssertionError( "cannot translate op " + op + " call " + call );
        }


        private boolean translateExpr( String op, RexNode left, RexNode right ) {
            if ( op == null ) {
                return false;
            }

            if ( left.isA( INPUT_REF ) && right.isA( INPUT_REF ) ) {
                BsonValue l = new BsonString( getPhysicalName( (RexInputRef) left ) );
                BsonValue r = new BsonString( getPhysicalName( (RexInputRef) right ) );

                dynamics.add( new BsonDocument( "$expr", new BsonDocument( op, new BsonArray( Arrays.asList( l, r ) ) ) ) );
                return true;
            }

            return false;
        }


        private boolean translateArray( String op, RexNode right, RexNode left ) {
            if ( right instanceof RexCall && left instanceof RexInputRef ) {
                // $9 ( index ) -> [el1, el2]
                String name = getPhysicalName( (RexInputRef) left );
                if ( op == null ) {
                    dynamics.add( new BsonDocument( "$expr", translateCall( name, (RexCall) right ) ) );
                } else {
                    dynamics.add( new BsonDocument( "$expr", new BsonDocument( op, translateCall( name, (RexCall) right ) ) ) );
                }
                return true;
            } else if ( right instanceof RexCall && left instanceof RexLiteral ) {
                if ( right.isA( DISTANCE ) ) {
                    translateFunction( op, (RexCall) right, left );

                } else {
                    // $9[1] -> el1
                    String name = getPhysicalName( (RexInputRef) ((RexCall) right).operands.get( 0 ) );
                    // we have to adjust as mongodb arrays start at 0 and sql at 1
                    int pos = ((RexLiteral) ((RexCall) right).operands.get( 1 )).getValueAs( Integer.class ) - 1;
                    translateOp2( null, name + "." + pos, (RexLiteral) left );
                }

                return true;
            }
            return false;
        }


        private BsonArray translateCall( String left, RexCall right ) {
            BsonArray array = new BsonArray();
            array.add( 0, new BsonString( "$" + left ) );
            array.add( getArray( right ) );
            return array;
        }


        private BsonDocument getArray( RexCall right ) {
            BsonArray array = new BsonArray( right.operands.stream().map( el -> {
                if ( el.isA( INPUT_REF ) ) {
                    return MongoTypeUtil.getAsBson( (RexLiteral) el, bucket );
                } else if ( el.isA( DYNAMIC_PARAM ) ) {
                    return new BsonDynamic( (RexDynamicParam) el );
                } else {
                    throw new RuntimeException( "Input in array is not translatable." );
                }
            } ).collect( Collectors.toList() ) );
            return new BsonDocument( getOp( right.op ), array );
        }


        private String getOp( SqlOperator op ) {
            switch ( op.kind ) {
                case PLUS:
                    return "$add";
                case MINUS:
                    return "$substr";
                case TIMES:
                    return "$multiply";
                case DIVIDE:
                    return "$divide";
                default:
                    throw new RuntimeException( "Sql operation is not supported" );
            }
        }


        private boolean translateFunction( String op, RexCall right, RexNode left ) {
            String randomName = getRandomName();
            this.preProjections.put( randomName, BsonFunctionHelper.getFunction( right, rowType, implementor ) );

            switch ( left.getKind() ) {
                case LITERAL:
                    if ( op == null ) {
                        this.dynamics.add( new BsonDocument( randomName, MongoTypeUtil.getAsBson( (RexLiteral) left, bucket ) ) );
                        return true;
                    } else {
                        this.dynamics.add( new BsonDocument( randomName, new BsonDocument( op, MongoTypeUtil.getAsBson( (RexLiteral) left, bucket ) ) ) );
                        return true;
                    }
                case DYNAMIC_PARAM:
                    if ( op == null ) {
                        this.dynamics.add( new BsonDocument( randomName, new BsonDynamic( (RexDynamicParam) left ) ) );
                        return true;
                    } else {
                        this.dynamics.add( new BsonDocument( randomName, new BsonDocument( op, new BsonDynamic( (RexDynamicParam) left ) ) ) );
                        return true;
                    }
                default:
                    return false;
            }

        }


        private String getRandomName() {
            return "__temp" + preProjections.size();
        }


        /**
         * Translates a call to a binary operator. Returns whether successful.
         */
        private boolean translateBinary2( String op, RexNode left, RexNode right ) {
            switch ( right.getKind() ) {
                case LITERAL:
                    return translateLiteral( op, left, (RexLiteral) right );
                case DYNAMIC_PARAM:
                    return translateDynamic( op, left, (RexDynamicParam) right );
                default:
                    return false;
            }
        }


        private boolean translateLiteral( String op, RexNode left, RexLiteral right ) {
            switch ( left.getKind() ) {
                case INPUT_REF:
                    translateOp2( op, getPhysicalName( (RexInputRef) left ), right );
                    return true;
                case CAST:
                    return translateBinary2( op, ((RexCall) left).operands.get( 0 ), right );
                case OTHER_FUNCTION:

                    String itemName = MongoRules.isItem( (RexCall) left );
                    if ( itemName != null ) {
                        translateOp2( op, itemName, right );
                        return true;
                    }
                    // fall through
                default:
                    return false;
            }
        }


        private boolean translateDynamic( String op, RexNode left, RexDynamicParam right ) {
            if ( left.getKind() == INPUT_REF ) {
                if ( op == null ) {
                    this.dynamics
                            .add(
                                    new BsonDocument()
                                            .append(
                                                    getPhysicalName( (RexInputRef) left ),
                                                    new BsonDynamic( right ) ) );
                } else {
                    this.dynamics
                            .add(
                                    new BsonDocument()
                                            .append(
                                                    getPhysicalName( (RexInputRef) left ),
                                                    new BsonDocument().append( op, new BsonDynamic( right ) ) ) );
                }
                return true;
            }
            if ( left.getKind() == DISTANCE ) {
                return translateFunction( op, (RexCall) left, right );
            }
            if ( left.getKind() == OTHER_FUNCTION ) {
                return translateItem( op, (RexCall) left, right );
            }

            return false;
        }


        private boolean translateItem( String op, RexCall left, RexDynamicParam right ) {
            if ( left.op instanceof SqlItemOperator ) {
                RexNode l = left.operands.get( 0 );
                RexNode r = left.operands.get( 1 );

                if ( l.isA( INPUT_REF ) ) {
                    BsonValue item;
                    if ( r.isA( LITERAL ) ) {
                        item = MongoTypeUtil.getAsBson( (RexLiteral) r, bucket );
                    } else if ( r.isA( DYNAMIC_PARAM ) ) {
                        item = new BsonDynamic( (RexDynamicParam) r );
                    } else {
                        return false;
                    }
                    String name = getRandomName();
                    BsonArray array = new BsonArray(
                            Arrays.asList(
                                    new BsonString( "$" + getPhysicalName( (RexInputRef) l ) ),
                                    new BsonDocument( "$add", new BsonArray( Arrays.asList( item, new BsonInt32( -1 ) ) ) ) ) );
                    this.preProjections.put( name, new BsonDocument( "$arrayElemAt", array ) );

                    this.dynamics.add( new BsonDocument( name, new BsonDynamic( right ) ) );

                    return true;
                }
            }
            return false;
        }


        private String getPhysicalName( RexInputRef input ) {
            String name = fieldNames.get( input.getIndex() );
            implementor.physicalMapper.add( name );
            return name;
        }


        private void translateOp2( String op, String name, RexLiteral right ) {
            if ( op == null ) {
                // E.g.: {deptno: 100}
                dynamics.add( new BsonDocument().append( name, MongoTypeUtil.getAsBson( right, bucket ) ) );
            } else {
                // E.g. {deptno: {$lt: 100}} which may later be combined with other conditions: E.g. {deptno: [$lt: 100, $gt: 50]}
                dynamics.add( new BsonDocument().append( name, new BsonDocument().append( op, MongoTypeUtil.getAsBson( right, bucket ) ) ) );
            }
        }

    }

}


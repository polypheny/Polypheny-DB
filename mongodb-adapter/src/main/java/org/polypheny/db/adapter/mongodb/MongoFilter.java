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


import static org.polypheny.db.sql.SqlKind.AND;
import static org.polypheny.db.sql.SqlKind.ARRAY_VALUE_CONSTRUCTOR;
import static org.polypheny.db.sql.SqlKind.CAST;
import static org.polypheny.db.sql.SqlKind.DISTANCE;
import static org.polypheny.db.sql.SqlKind.DOC_VALUE;
import static org.polypheny.db.sql.SqlKind.DYNAMIC_PARAM;
import static org.polypheny.db.sql.SqlKind.INPUT_REF;
import static org.polypheny.db.sql.SqlKind.LITERAL;
import static org.polypheny.db.sql.SqlKind.OR;
import static org.polypheny.db.sql.SqlKind.OTHER_FUNCTION;

import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonRegularExpression;
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
        private final List<BsonDocument> ors = new ArrayList<>();
        private final GridFSBucket bucket;
        private final BsonDocument preProjections = new BsonDocument();
        private final Implementor implementor;
        private boolean useTempFlag = false;
        private Map<String, List<BsonValue>> map = new HashMap<>();


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
            BsonDocument value = translateFinalOr( condition );
            if ( !value.isEmpty() ) {
                implementor.filter.add( value );
            }

            if ( preProjections.size() != 0 ) {
                implementor.preProjections.add( preProjections );
            }
        }


        private BsonDocument translateFinalOr( RexNode condition ) {
            for ( RexNode node : RelOptUtil.disjunctions( condition ) ) {
                HashMap<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
                this.map = new HashMap<>();
                translateAnd( node );

                mergeMaps( this.map, shallowCopy, "$or" );
                this.map = shallowCopy;
            }

            return asConditionDocument( this.map );
        }


        private BsonDocument asConditionDocument( Map<String, List<BsonValue>> map ) {
            if ( map.size() == 0 ) {
                return new BsonDocument();
            }
            BsonDocument doc = new BsonDocument();
            List<BsonValue> ands = new ArrayList<>();
            for ( Entry<String, List<BsonValue>> entry : map.entrySet() ) {
                if ( entry.getValue().size() == 1 && !entry.getKey().equals( "$or" ) && !entry.getKey().equals( "$and" ) ) {
                    doc.put( entry.getKey(), entry.getValue().get( 0 ) );
                } else if ( entry.getKey().equals( "$or" ) || entry.getKey().equals( "$and" ) ) {
                    doc.put( entry.getKey(), new BsonArray( entry.getValue() ) );
                } else {
                    ands.addAll( entry.getValue().stream().map( e -> new BsonDocument( entry.getKey(), e ) ).collect( Collectors.toList() ) );
                }
            }
            if ( ands.size() != 0 ) {
                doc.put( "$and", new BsonArray( ands ) );
            }
            return doc;
        }


        private BsonValue asCondition( List<BsonValue> bsonValues ) {
            if ( bsonValues.size() == 1 ) {
                return bsonValues.get( 0 );
            } else {
                return new BsonDocument( "$or", new BsonArray( bsonValues ) );
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


        private void translateMatch2( RexNode node ) {
            switch ( node.getKind() ) {
                case EQUALS:
                    translateBinary( null, null, (RexCall) node );
                    return;
                case LESS_THAN:
                    translateBinary( "$lt", "$gt", (RexCall) node );
                    return;
                case LESS_THAN_OR_EQUAL:
                    translateBinary( "$lte", "$gte", (RexCall) node );
                    return;
                case NOT_EQUALS:
                    translateBinary( "$ne", "$ne", (RexCall) node );
                    return;
                case GREATER_THAN:
                    translateBinary( "$gt", "$lt", (RexCall) node );
                    return;
                case GREATER_THAN_OR_EQUAL:
                    translateBinary( "$gte", "$lte", (RexCall) node );
                    return;
                case LIKE:
                    translateLike( (RexCall) node );
                    return;
                case DOC_SIZE_MATCH:
                    translateSize( (RexCall) node );
                    return;
                case DOC_REGEX_MATCH:
                    translateRegex( (RexCall) node );
                    return;
                case DOC_TYPE_MATCH:
                    translateTypeMatch( (RexCall) node );
                    return;
                case DOC_ELEM_MATCH:
                    translateElemMatch( (RexCall) node );
                    return;
                case NOT:
                    translateNot( (RexCall) node );
                    return;
                case OR:
                    translateOr( (RexCall) node );
                    return;
                case DOC_EXISTS:
                    translateExists( (RexCall) node );
                    return;
                default:
                    throw new AssertionError( "cannot translate " + node );
            }
        }


        private Void translateOr( RexCall node ) {
            Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
            this.map = new HashMap<>();

            translateNode( node.operands.get( 0 ) );

            mergeMaps( this.map, shallowCopy, "$or" );
            this.map = shallowCopy;

            return null;
        }


        private void translateNot( RexCall node ) {
            Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
            this.map = new HashMap<>();

            translateNode( node.operands.get( 0 ) );

            mergeMaps( this.map, shallowCopy, "$not" );
            this.map = shallowCopy;

        }


        private void translateNode( RexNode node0 ) {
            if ( node0.getKind() == AND ) {
                translateAnd( node0 );
            } else if ( node0.getKind() == OR ) {
                translateFinalOr( node0 );
            } else {
                translateMatch2( node0 );
            }
        }


        private void mergeMaps( Map<String, List<BsonValue>> newValueMap, Map<String, List<BsonValue>> finalValueMap, String op ) {
            if ( !op.equals( "$or" ) ) {
                newValueMap
                        .replaceAll( ( k, v ) -> v.stream().map( e -> new BsonDocument( "$not", e ) )
                                .collect( Collectors.toList() ) );

                for ( Entry<String, List<BsonValue>> values : newValueMap.entrySet() ) {
                    if ( finalValueMap.containsKey( values.getKey() ) ) {
                        finalValueMap.get( values.getKey() ).add( new BsonDocument( op, asCondition( values.getValue() ) ) );
                    } else {
                        List<BsonValue> bsons = new ArrayList<>();
                        bsons.add( new BsonDocument( op, asCondition( values.getValue() ) ) );
                        finalValueMap.put( values.getKey(), bsons );
                    }
                }
            } else {
                List<BsonValue> ors = new ArrayList<>();

                for ( Entry<String, List<BsonValue>> entry : newValueMap.entrySet() ) {
                    if ( !entry.getKey().equals( "$or" ) ) {
                        List<BsonValue> ands = new ArrayList<>();
                        for ( BsonValue value : entry.getValue() ) {
                            ands.add( new BsonDocument( entry.getKey(), value ) );
                        }
                        ors.add( new BsonDocument( "$and", new BsonArray( ands ) ) );
                    } else {
                        ors.addAll( entry.getValue() );
                    }
                }

                if ( finalValueMap.containsKey( "$or" ) ) {
                    finalValueMap.get( "$or" ).addAll( ors );
                } else {
                    finalValueMap.put( "$or", ors );
                }
            }
        }


        private void translateExists( RexCall node ) {
            assert node.operands.size() == 2;
            assert node.operands.get( 1 ) instanceof RexCall;
            String key = getParamAsKey( node.operands.get( 0 ) );
            key += "." + ((RexCall) node.operands.get( 1 ))
                    .operands
                    .stream()
                    .map( o -> ((RexLiteral) o).getValueAs( String.class ) )
                    .collect( Collectors.joining( "." ) );
            attachCondition( "$exists", key, new BsonBoolean( true ) );
        }


        private Void translateElemMatch( RexCall node ) {
            if ( node.operands.size() != 2 ) {
                return null;
            }
            String key = getParamAsKey( node.operands.get( 0 ) );

            Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
            this.map = new HashMap<>();

            translateNode( node.operands.get( 1 ) );

            mergeMaps( this.map, shallowCopy, "$elemMatch" );
            this.map = shallowCopy;

            return null;
        }


        private void translateTypeMatch( RexCall node ) {
            if ( node.operands.size() != 2
                    || !(node.operands.get( 1 ) instanceof RexCall)
                    || ((RexCall) node.operands.get( 1 )).op.kind != ARRAY_VALUE_CONSTRUCTOR ) {
                return;
            }

            String key = getParamAsKey( node.operands.get( 0 ) );
            List<BsonValue> types = ((RexCall) node.operands.get( 1 )).operands
                    .stream()
                    .map( el -> ((RexLiteral) el).getValueAs( Integer.class ) )
                    .map( BsonInt32::new )
                    .collect( Collectors.toList() );
            attachCondition( "$type", key, new BsonArray( types ) );
        }


        private void translateRegex( RexCall node ) {
            if ( node.operands.size() != 6 ) {
                return;
            }
            String left = getParamAsKey( node.operands.get( 0 ) );
            String value = getLiteralAs( node, 1, String.class );

            boolean isInsensitive = getLiteralAs( node, 2, Boolean.class );
            boolean isMultiline = getLiteralAs( node, 3, Boolean.class );
            boolean doesIgnoreWhitespace = getLiteralAs( node, 4, Boolean.class );
            boolean allowsDot = getLiteralAs( node, 5, Boolean.class );

            String options = (isInsensitive ? "i" : "")
                    + (isMultiline ? "m" : "")
                    + (doesIgnoreWhitespace ? "x" : "")
                    + (allowsDot ? "s" : "");

            attachCondition( null, left, new BsonRegularExpression( value, options ) );
        }


        private <E> E getLiteralAs( RexCall node, int pos, Class<E> clazz ) {
            return ((RexLiteral) node.operands.get( pos )).getValueAs( clazz );
        }


        private void translateSize( RexCall node ) {
            if ( node.operands.size() != 2 ) {
                return;
            }
            String left = getParamAsKey( node.operands.get( 0 ) );
            BsonValue value = getParamAsValue( node.operands.get( 1 ) );
            attachCondition( null, left, new BsonDocument( "$size", value ) );

        }


        private String translateDocValue( RexCall call ) {
            RexInputRef parent = (RexInputRef) call.getOperands().get( 0 );
            RexCall names = (RexCall) call.operands.get( 1 );
            return rowType.getFieldNames().get( parent.getIndex() )
                    + "."
                    + names.operands
                    .stream()
                    .map( n -> ((RexLiteral) n).getValueAs( String.class ) )
                    .collect( Collectors.joining( "." ) );
        }


        private String getParamAsKey( RexNode node ) {

            if ( node.isA( INPUT_REF ) ) {
                return rowType.getFieldNames().get( ((RexInputRef) node).getIndex() );
            } else {
                return translateDocValue( (RexCall) node );
            }
        }


        @Nullable
        private BsonValue getParamAsValue( RexNode node ) {

            if ( node.isA( INPUT_REF ) ) {
                return new BsonString( "$" + rowType.getFieldNames().get( ((RexInputRef) node).getIndex() ) );
            } else if ( node.isA( DYNAMIC_PARAM ) ) {
                return new BsonDynamic( (RexDynamicParam) node );
            }
            return null;
        }


        private void translateLike( RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );

            switch ( right.getKind() ) {
                case DYNAMIC_PARAM:
                    attachCondition( null, getPhysicalName( (RexInputRef) left ),
                            new BsonDynamic( (RexDynamicParam) right ).setIsRegex( true ) );
                    break;

                case LITERAL:
                    attachCondition( null, getPhysicalName( (RexInputRef) left ),
                            MongoTypeUtil.replaceLikeWithRegex( ((RexLiteral) right).getValueAs( String.class ) ) );
                    break;

                default:
                    throw new IllegalStateException( "Unexpected value: " + right.getKind() );
            }
        }


        /**
         * Translates a call to a binary operator, reversing arguments if necessary.
         */
        private void translateBinary( String op, String rop, RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            boolean b = translateBinary2( op, left, right );
            if ( b ) {
                return;
            }
            b = translateBinary2( rop, right, left );
            if ( b ) {
                return;
            }

            b = translateArray( op, left, right );
            if ( b ) {
                return;
            }

            b = translateArray( op, right, left );
            if ( b ) {
                return;
            }

            b = translateExpr( op, left, right );
            if ( b ) {
                return;
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

                attachCondition( null, "$expr", new BsonDocument( op, new BsonArray( Arrays.asList( l, r ) ) ) );
                return true;
            }

            return false;
        }


        private boolean translateArray( String op, RexNode right, RexNode left ) {
            if ( right instanceof RexCall && left instanceof RexInputRef ) {
                // $9 ( index ) -> [el1, el2]
                String name = getPhysicalName( (RexInputRef) left );
                attachCondition( op, "$expr", translateCall( name, (RexCall) right ) );
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
                } else if ( el.isA( LITERAL ) ) {
                    return MongoTypeUtil.getAsBson( (RexLiteral) el, bucket );
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
                case ARRAY_VALUE_CONSTRUCTOR:
                    return "$eq";
                default:
                    throw new RuntimeException( "Sql operation is not supported" );
            }
        }


        private boolean translateFunction( String op, RexCall right, RexNode left ) {
            String randomName = getRandomName();
            this.preProjections.put( randomName, BsonFunctionHelper.getFunction( right, rowType, implementor ) );

            switch ( left.getKind() ) {
                case LITERAL:
                    attachCondition( op, randomName, MongoTypeUtil.getAsBson( (RexLiteral) left, bucket ) );
                    return true;
                case DYNAMIC_PARAM:
                    attachCondition( op, randomName, new BsonDynamic( (RexDynamicParam) left ) );
                    return true;
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

                case DOC_VALUE:
                    return translateDocValue( op, (RexCall) left, right );

                // fall through

                default:
                    return false;
            }
        }


        private boolean translateDynamic( String op, RexNode left, RexDynamicParam right ) {
            if ( left.getKind() == INPUT_REF ) {
                attachCondition( op, getPhysicalName( (RexInputRef) left ), new BsonDynamic( right ) );
                return true;
            }
            if ( left.getKind() == DISTANCE ) {
                return translateFunction( op, (RexCall) left, right );
            }
            if ( left.getKind() == OTHER_FUNCTION ) {
                return translateItem( op, (RexCall) left, right );
            }
            if ( left.getKind() == DOC_VALUE ) {
                return translateDocValue( op, (RexCall) left, right );
            }
            if ( left.getKind() == CAST ) {
                return translateDynamic( op, ((RexCall) left).operands.get( 0 ), right );
            }

            return false;
        }


        private boolean translateDocValue( String op, RexCall left, RexNode right ) {
            BsonValue item = getItem( right );
            if ( item == null ) {
                return false;
            }
            if ( !left.getOperands().get( 0 ).isA( INPUT_REF )
                    || left.operands.size() != 2
                    || !(left.operands.get( 1 ) instanceof RexCall)
                    || !left.getOperands().get( 1 ).isA( ARRAY_VALUE_CONSTRUCTOR ) ) {
                return false;
            }
            RexInputRef parent = (RexInputRef) left.getOperands().get( 0 );
            RexCall names = (RexCall) left.operands.get( 1 );
            String mergedName = rowType.getFieldNames().get( parent.getIndex() );

            if ( names.operands.size() > 0 ) {
                mergedName += "." + names.operands
                        .stream()
                        .map( name -> ((RexLiteral) name).getValueAs( String.class ) )
                        .collect( Collectors.joining( "." ) );
            }

            attachCondition( op, mergedName, item );
            return true;
        }


        private boolean translateItem( String op, RexCall left, RexDynamicParam right ) {
            if ( left.op instanceof SqlItemOperator ) {
                RexNode l = left.operands.get( 0 );
                RexNode r = left.operands.get( 1 );

                if ( l.isA( INPUT_REF ) ) {
                    BsonValue item = getItem( r );
                    if ( item == null ) {
                        return false;
                    }
                    String name = getRandomName();
                    BsonArray array = new BsonArray(
                            Arrays.asList(
                                    new BsonString( "$" + getPhysicalName( (RexInputRef) l ) ),
                                    new BsonDocument( "$add", new BsonArray( Arrays.asList( item, new BsonInt32( -1 ) ) ) ) ) );
                    this.preProjections.put( name, new BsonDocument( "$arrayElemAt", array ) );

                    attachCondition( null, name, new BsonDynamic( right ) );

                    return true;
                }
            }
            return false;
        }


        @Nullable
        private BsonValue getItem( RexNode r ) {
            BsonValue item;
            if ( r.isA( LITERAL ) ) {
                item = MongoTypeUtil.getAsBson( (RexLiteral) r, bucket );
            } else if ( r.isA( DYNAMIC_PARAM ) ) {
                item = new BsonDynamic( (RexDynamicParam) r );
            } else {
                return null;
            }
            return item;
        }


        private String getPhysicalName( RexInputRef input ) {
            String name = fieldNames.get( input.getIndex() );
            // DML (and also DDL) have to use the physical name, as they do not allow
            // to use projections beforehand
            if ( implementor.isDML() ) {
                if ( rowType != null && rowType.getId( name ) != null ) {
                    name = rowType.getPhysicalName( name, implementor );
                }
                return name;
            }
            implementor.physicalMapper.add( name );
            return name;
        }


        private void translateOp2( String op, String name, RexLiteral right ) {
            attachCondition( op, name, MongoTypeUtil.getAsBson( right, bucket ) );
        }


        private void attachCondition( String op, String name, BsonValue right ) {
            if ( op == null ) {
                // E.g.: {deptno: 100}
                if ( map.containsKey( name ) ) {
                    map.get( name ).add( right );
                } else {
                    map.put( name, new ArrayList<>( Collections.singletonList( right ) ) );
                }
            } else {
                // E.g. {deptno: {$lt: 100}} which may later be combined with other conditions: E.g. {deptno: [$lt: 100, $gt: 50]}
                if ( map.containsKey( name ) ) {
                    map.get( name ).add( new BsonDocument( op, right ) );
                } else {
                    map.put( name, new ArrayList<>( Collections.singletonList( new BsonDocument( op, right ) ) ) );
                }
            }
        }

    }

}


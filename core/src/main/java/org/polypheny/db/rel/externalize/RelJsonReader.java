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

package org.polypheny.db.rel.externalize;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;


/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see RelInput
 */
public class RelJsonReader {

    private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF = new TypeReference<LinkedHashMap<String, Object>>() {
    };

    private final RelOptCluster cluster;
    private final RelOptSchema relOptSchema;
    private final RelJson relJson = new RelJson( null );
    private final Map<String, RelNode> relMap = new LinkedHashMap<>();
    private RelNode lastRel;


    public RelJsonReader( RelOptCluster cluster, RelOptSchema relOptSchema, Schema schema ) {
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
        Util.discard( schema );
    }


    public RelNode read( String s ) throws IOException {
        lastRel = null;
        final ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> o = mapper.readValue( s, TYPE_REF );
        @SuppressWarnings("unchecked") final Map<String, Object> rels = (Map) o.get( "Plan" );
        readRels( rels );
        System.out.println( lastRel );
        return lastRel;
    }


    private void readRels( Map<String, Object> jsonRels ) {
        @SuppressWarnings("unchecked") List<Map<String, Object>> inputsList = (List) jsonRels.get( "inputs" );
        for ( Map<String, Object> input : inputsList ) {
            readRels( input );
        }
        readRel( jsonRels );
    }


    private void readRel( final Map<String, Object> jsonRel ) {
        String id = (String) jsonRel.get( "id" );
        String type = (String) jsonRel.get( "relOp" );
        Constructor constructor = relJson.getConstructor( type );
        RelInput input = new RelInput() {
            @Override
            public RelOptCluster getCluster() {
                return cluster;
            }


            @Override
            public RelTraitSet getTraitSet() {
                return cluster.traitSetOf( Convention.NONE );
            }


            @Override
            public RelOptTable getTable( String table ) {
                final List<String> list;
                if ( jsonRel.get( table ) instanceof String ) {
                    String str = (String) jsonRel.get( table );
                    // MV: This is not a nice solution...
                    if ( str.startsWith( "[" ) && str.endsWith( "]" ) ) {
                        str = str.substring( 1, str.length() - 1 );
                        list = new LinkedList<>();
                        list.add( StringUtils.join( Arrays.asList( str.split( "," ) ), ", " ) );
                    } else {
                        list = getStringList( table );
                    }
                } else {
                    list = getStringList( table );
                }
                return relOptSchema.getTableForMember( list );
            }


            @Override
            public RelNode getInput() {
                final List<RelNode> inputs = getInputs();
                assert inputs.size() == 1;
                return inputs.get( 0 );
            }


            @Override
            public List<RelNode> getInputs() {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonInputs = (List) get( "inputs" );
                if ( jsonInputs == null ) {
                    return ImmutableList.of( lastRel );
                }
                final List<RelNode> inputs = new ArrayList<>();
                for ( Map<String, Object> jsonInput : jsonInputs ) {
                    inputs.add( lookupInput( jsonInput ) );
                }
                return inputs;
            }


            @Override
            public RexNode getExpression( String tag ) {
                return relJson.toRex( this, jsonRel.get( tag ) );
            }


            @Override
            public ImmutableBitSet getBitSet( String tag ) {
                return ImmutableBitSet.of( getIntegerList( tag ) );
            }


            @Override
            public List<ImmutableBitSet> getBitSetList( String tag ) {
                List<List<Integer>> list = getIntegerListList( tag );
                if ( list == null ) {
                    return null;
                }
                final ImmutableList.Builder<ImmutableBitSet> builder = ImmutableList.builder();
                for ( List<Integer> integers : list ) {
                    builder.add( ImmutableBitSet.of( integers ) );
                }
                return builder.build();
            }


            @Override
            public List<String> getStringList( String tag ) {
                //noinspection unchecked
                return (List<String>) jsonRel.get( tag );
            }


            @Override
            public List<Integer> getIntegerList( String tag ) {
                //noinspection unchecked
                return (List<Integer>) jsonRel.get( tag );
            }


            @Override
            public List<List<Integer>> getIntegerListList( String tag ) {
                //noinspection unchecked
                return (List<List<Integer>>) jsonRel.get( tag );
            }


            @Override
            public List<AggregateCall> getAggregateCalls( String tag ) {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonAggs = (List) jsonRel.get( tag );
                final List<AggregateCall> inputs = new ArrayList<>();
                for ( Map<String, Object> jsonAggCall : jsonAggs ) {
                    inputs.add( toAggCall( jsonAggCall ) );
                }
                return inputs;
            }


            @Override
            public Object get( String tag ) {
                return jsonRel.get( tag );
            }


            @Override
            public String getString( String tag ) {
                return (String) jsonRel.get( tag );
            }


            @Override
            public float getFloat( String tag ) {
                return ((Number) jsonRel.get( tag )).floatValue();
            }


            @Override
            public boolean getBoolean( String tag, boolean default_ ) {
                final Boolean b = (Boolean) jsonRel.get( tag );
                return b != null ? b : default_;
            }


            @Override
            public <E extends Enum<E>> E getEnum( String tag, Class<E> enumClass ) {
                return Util.enumVal( enumClass, getString( tag ).toUpperCase( Locale.ROOT ) );
            }


            @Override
            public List<RexNode> getExpressionList( String tag ) {
                @SuppressWarnings("unchecked") final List<Object> jsonNodes = (List) jsonRel.get( tag );
                final List<RexNode> nodes = new ArrayList<>();
                for ( Object jsonNode : jsonNodes ) {
                    nodes.add( relJson.toRex( this, jsonNode ) );
                }
                return nodes;
            }


            @Override
            public RelDataType getRowType( String tag ) {
                final Object o = jsonRel.get( tag );
                return relJson.toType( cluster.getTypeFactory(), o );
            }


            @Override
            public RelDataType getRowType( String expressionsTag, String fieldsTag ) {
                final List<RexNode> expressionList = getExpressionList( expressionsTag );
                @SuppressWarnings("unchecked") final List<String> names = (List<String>) get( fieldsTag );
                return cluster.getTypeFactory().createStructType(
                        new AbstractList<Map.Entry<String, RelDataType>>() {
                            @Override
                            public Map.Entry<String, RelDataType> get( int index ) {
                                return Pair.of( names.get( index ), expressionList.get( index ).getType() );
                            }


                            @Override
                            public int size() {
                                return names.size();
                            }
                        } );
            }


            @Override
            public RelCollation getCollation() {
                //noinspection unchecked
                return relJson.toCollation( (List) get( "collation" ) );
            }


            @Override
            public RelDistribution getDistribution() {
                return relJson.toDistribution( get( "distribution" ) );
            }


            @Override
            public ImmutableList<ImmutableList<RexLiteral>> getTuples( String tag ) {
                //noinspection unchecked
                final List<List> jsonTuples = (List) get( tag );
                final ImmutableList.Builder<ImmutableList<RexLiteral>> builder = ImmutableList.builder();
                for ( List jsonTuple : jsonTuples ) {
                    builder.add( getTuple( jsonTuple ) );
                }
                return builder.build();
            }


            public ImmutableList<RexLiteral> getTuple( List jsonTuple ) {
                final ImmutableList.Builder<RexLiteral> builder = ImmutableList.builder();
                for ( Object jsonValue : jsonTuple ) {
                    builder.add( (RexLiteral) relJson.toRex( this, jsonValue ) );
                }
                return builder.build();
            }
        };
        try {
            final RelNode rel = (RelNode) constructor.newInstance( input );
            relMap.put( id, rel );
            lastRel = rel;
        } catch ( InstantiationException | IllegalAccessException e ) {
            throw new RuntimeException( e );
        } catch ( InvocationTargetException e ) {
            final Throwable e2 = e.getCause();
            if ( e2 instanceof RuntimeException ) {
                throw (RuntimeException) e2;
            }
            throw new RuntimeException( e2 );
        }
    }


    private AggregateCall toAggCall( Map<String, Object> jsonAggCall ) {
        final String aggName = (String) jsonAggCall.get( "agg" );
        final SqlAggFunction aggregation = relJson.toAggregation( aggName, jsonAggCall );
        final Boolean distinct = (Boolean) jsonAggCall.get( "distinct" );
        @SuppressWarnings("unchecked") final List<Integer> operands = (List<Integer>) jsonAggCall.get( "operands" );
        final Integer filterOperand = (Integer) jsonAggCall.get( "filter" );
        final RelDataType type = relJson.toType( cluster.getTypeFactory(), jsonAggCall.get( "type" ) );
        return AggregateCall.create( aggregation, distinct, false, operands, filterOperand == null ? -1 : filterOperand, RelCollations.EMPTY, type, null );
    }


    private RelNode lookupInput( Map<String, Object> jsonInput ) {
        String id = (String) jsonInput.get( "id" );
        RelNode node = relMap.get( id );
        if ( node == null ) {
            throw new RuntimeException( "unknown id " + id + " for relational expression" );
        }
        return node;
    }
}


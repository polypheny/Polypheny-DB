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

package org.polypheny.db.algebra.externalize;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Reads a JSON plan and converts it back to a tree of relational expressions.
 *
 * @see AlgInput
 */
public class AlgJsonReader {

    private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF = new TypeReference<>() {
    };

    private final AlgOptCluster cluster;
    private final AlgOptSchema algOptSchema;
    private final AlgJson algJson = new AlgJson( null );
    private final Map<String, AlgNode> algMap = new LinkedHashMap<>();
    private AlgNode lastAlg;


    public AlgJsonReader( AlgOptCluster cluster, AlgOptSchema algOptSchema, Schema schema ) {
        this.cluster = cluster;
        this.algOptSchema = algOptSchema;
        Util.discard( schema );
    }


    public AlgNode read( String s ) throws IOException {
        lastAlg = null;
        final ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> o = mapper.readValue( s, TYPE_REF );
        @SuppressWarnings("unchecked") final Map<String, Object> algs = (Map) o.get( "Plan" );
        readRels( algs );
        System.out.println( lastAlg );
        return lastAlg;
    }


    private void readRels( Map<String, Object> jsonRels ) {
        @SuppressWarnings("unchecked") List<Map<String, Object>> inputsList = (List) jsonRels.get( "inputs" );
        for ( Map<String, Object> input : inputsList ) {
            readRels( input );
        }
        readAlg( jsonRels );
    }


    private void readAlg( final Map<String, Object> jsonAlg ) {
        String id = (String) jsonAlg.get( "id" );
        String type = (String) jsonAlg.get( "relOp" );
        Constructor constructor = algJson.getConstructor( type );
        AlgInput input = new AlgInput() {
            @Override
            public AlgOptCluster getCluster() {
                return cluster;
            }


            @Override
            public AlgTraitSet getTraitSet() {
                return cluster.traitSetOf( Convention.NONE );
            }


            @Override
            public AlgOptTable getTable( String table ) {
                final List<String> list;
                if ( jsonAlg.get( table ) instanceof String ) {
                    String str = (String) jsonAlg.get( table );
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
                return algOptSchema.getTableForMember( list );
            }


            @Override
            public AlgNode getInput() {
                final List<AlgNode> inputs = getInputs();
                assert inputs.size() == 1;
                return inputs.get( 0 );
            }


            @Override
            public List<AlgNode> getInputs() {
                @SuppressWarnings("unchecked") final List<Map<String, Object>> jsonInputs = (List) get( "inputs" );
                if ( jsonInputs == null ) {
                    return ImmutableList.of( lastAlg );
                }
                final List<AlgNode> inputs = new ArrayList<>();
                for ( Map<String, Object> jsonInput : jsonInputs ) {
                    inputs.add( lookupInput( jsonInput ) );
                }
                return inputs;
            }


            @Override
            public ImmutableBitSet getBitSet( String tag ) {
                return ImmutableBitSet.of( getIntegerList( tag ) );
            }


            @Override
            public List<String> getStringList( String tag ) {
                //noinspection unchecked
                return (List<String>) jsonAlg.get( tag );
            }


            @Override
            public List<Integer> getIntegerList( String tag ) {
                //noinspection unchecked
                return (List<Integer>) jsonAlg.get( tag );
            }


            @Override
            public List<List<Integer>> getIntegerListList( String tag ) {
                //noinspection unchecked
                return (List<List<Integer>>) jsonAlg.get( tag );
            }


            @Override
            public Object get( String tag ) {
                return jsonAlg.get( tag );
            }


            @Override
            public String getString( String tag ) {
                return (String) jsonAlg.get( tag );
            }


            @Override
            public float getFloat( String tag ) {
                return ((Number) jsonAlg.get( tag )).floatValue();
            }


            @Override
            public boolean getBoolean( String tag, boolean default_ ) {
                final Boolean b = (Boolean) jsonAlg.get( tag );
                return b != null ? b : default_;
            }


            @Override
            public <E extends Enum<E>> E getEnum( String tag, Class<E> enumClass ) {
                return Util.enumVal( enumClass, getString( tag ).toUpperCase( Locale.ROOT ) );
            }


            @Override
            public AlgDataType getRowType( String tag ) {
                final Object o = jsonAlg.get( tag );
                return algJson.toType( cluster.getTypeFactory(), o );
            }


            @Override
            public AlgCollation getCollation() {
                //noinspection unchecked
                return algJson.toCollation( (List) get( "collation" ) );
            }


            @Override
            public AlgDistribution getDistribution() {
                return algJson.toDistribution( get( "distribution" ) );
            }

        };
        try {
            final AlgNode alg = (AlgNode) constructor.newInstance( input );
            algMap.put( id, alg );
            lastAlg = alg;
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


    private AlgNode lookupInput( Map<String, Object> jsonInput ) {
        String id = (String) jsonInput.get( "id" );
        AlgNode node = algMap.get( id );
        if ( node == null ) {
            throw new RuntimeException( "unknown id " + id + " for relational expression" );
        }
        return node;
    }

}


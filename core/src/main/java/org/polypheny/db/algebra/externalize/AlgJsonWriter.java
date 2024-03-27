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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.util.JsonBuilder;
import org.polypheny.db.util.Pair;


/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see AlgJsonReader
 */
public class AlgJsonWriter implements AlgWriter {

    private final JsonBuilder jsonBuilder;
    private final AlgJson algJson;
    private final Map<AlgNode, String> algIdMap = new IdentityHashMap<>();
    private final List<Object> algList;
    private final List<Pair<String, Object>> values = new ArrayList<>();
    private String previousId;

    private final Map<String, Map<String, Object>> parts;


    public AlgJsonWriter() {
        jsonBuilder = new JsonBuilder();
        algList = jsonBuilder.list();
        algJson = new AlgJson( jsonBuilder );
        parts = new HashMap<>();
    }


    protected void explain_( AlgNode alg, List<Pair<String, Object>> values ) {
        final Map<String, Object> map = jsonBuilder.map();
        final AlgMetadataQuery mq = alg.getCluster().getMetadataQuery();

        map.put( "id", null ); // ensure that id is the first attribute
        map.put( "relOp", algJson.classToTypeName( alg.getClass() ) );
        for ( Pair<String, Object> value : values ) {
            if ( value.right instanceof AlgNode ) {
                continue;
            }
            put( map, value.left, replaceWithFieldNames( alg, value.right ) );
        }

        put( map, "rowcount", mq.getTupleCount( alg ) );
        try {
            put( map, "rows cost", mq.getCumulativeCost( alg ).getRows() );
            put( map, "cpu cost", mq.getCumulativeCost( alg ).getCpu() );
            put( map, "io cost", mq.getCumulativeCost( alg ).getIo() );
        } catch ( Exception e ) {
            put( map, "rows cost", "unknown" );
            put( map, "cpu cost", "unknown" );
            put( map, "io cost", "unknown" );
        }

        final List<Object> list = explainInputs( alg.getInputs() );
        List<Object> l = new LinkedList<>();
        for ( Object o : list ) {
            l.add( parts.get( o ) );
        }
        map.put( "inputs", l );

        final String id = Integer.toString( algIdMap.size() );
        algIdMap.put( alg, id );
        map.put( "id", id );

        algList.add( map );
        parts.put( id, map );
        previousId = id;
    }


    private String replaceWithFieldNames( AlgNode alg, Object right ) {
        String str = right == null ? "" : right.toString();
        if ( str.contains( "$" ) ) {
            int offset = 0;
            for ( AlgNode input : alg.getInputs() ) {
                for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
                    String searchStr = "$" + (offset + field.getIndex());
                    int position = str.indexOf( searchStr );
                    if ( position >= 0 && (str.length() >= position + searchStr.length()) ) {
                        str = str.replace( searchStr, field.getName() );
                    }
                }
                offset = input.getTupleType().getFields().size();
            }
        }
        return str;
    }


    private void put( Map<String, Object> map, String name, Object value ) {
        map.put( name, algJson.toJson( value ) );
    }


    private List<Object> explainInputs( List<AlgNode> inputs ) {
        final List<Object> list = jsonBuilder.list();
        for ( AlgNode input : inputs ) {
            String id = algIdMap.get( input );
            if ( id == null ) {
                input.explain( this );
                id = previousId;
            }
            list.add( id );
        }
        return list;
    }


    @Override
    public final void explain( AlgNode alg, List<Pair<String, Object>> valueList ) {
        explain_( alg, valueList );
    }


    @Override
    public ExplainLevel getDetailLevel() {
        return ExplainLevel.ALL_ATTRIBUTES;
    }


    @Override
    public AlgWriter input( String term, AlgNode input ) {
        return this;
    }


    @Override
    public AlgWriter item( String term, Object value ) {
        values.add( Pair.of( term, value ) );
        return this;
    }


    private List<Object> getList( List<Pair<String, Object>> values, String tag ) {
        for ( Pair<String, Object> value : values ) {
            if ( value.left.equals( tag ) ) {
                //noinspection unchecked
                return (List<Object>) value.right;
            }
        }
        final List<Object> list = new ArrayList<>();
        values.add( Pair.of( tag, (Object) list ) );
        return list;
    }


    @Override
    public AlgWriter itemIf( String term, Object value, boolean condition ) {
        if ( condition ) {
            item( term, value );
        }
        return this;
    }


    @Override
    public AlgWriter done( AlgNode node ) {
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf( values );
        values.clear();
        explain_( node, valuesCopy );
        return this;
    }


    @Override
    public boolean nest() {
        return true;
    }


    /**
     * Returns a JSON string describing the relational expressions that were just explained.
     */
    public String asString() {
        final Map<String, Object> map = jsonBuilder.map();
        map.put( "Plan", algList.get( algList.size() - 1 ) );
        return jsonBuilder.toJsonString( map );
    }

}

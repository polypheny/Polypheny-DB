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


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.util.JsonBuilder;
import org.polypheny.db.util.Pair;


/**
 * Callback for a relational expression to dump itself as JSON.
 *
 * @see RelJsonReader
 */
public class RelJsonWriter implements RelWriter {

    private final JsonBuilder jsonBuilder;
    private final RelJson relJson;
    private final Map<RelNode, String> relIdMap = new IdentityHashMap<>();
    private final List<Object> relList;
    private final List<Pair<String, Object>> values = new ArrayList<>();
    private String previousId;

    private final Map<String, Map<String, Object>> parts;


    public RelJsonWriter() {
        jsonBuilder = new JsonBuilder();
        relList = jsonBuilder.list();
        relJson = new RelJson( jsonBuilder );
        parts = new HashMap<>();
    }


    protected void explain_( RelNode rel, List<Pair<String, Object>> values ) {
        final Map<String, Object> map = jsonBuilder.map();
        final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

        map.put( "id", null ); // ensure that id is the first attribute
        map.put( "relOp", relJson.classToTypeName( rel.getClass() ) );
        for ( Pair<String, Object> value : values ) {
            if ( value.right instanceof RelNode ) {
                continue;
            }
            put( map, value.left, replaceWithFieldNames( rel, value.right ) );
        }

        put( map, "rowcount", mq.getRowCount( rel ) );
        put( map, "rows cost", mq.getCumulativeCost( rel ).getRows() );
        put( map, "cpu cost", mq.getCumulativeCost( rel ).getCpu() );
        put( map, "io cost", mq.getCumulativeCost( rel ).getIo() );

        final List<Object> list = explainInputs( rel.getInputs() );
        List<Object> l = new LinkedList<>();
        for ( Object o : list ) {
            l.add( parts.get( o ) );
        }
        map.put( "inputs", l );

        final String id = Integer.toString( relIdMap.size() );
        relIdMap.put( rel, id );
        map.put( "id", id );

        relList.add( map );
        parts.put( id, map );
        previousId = id;
    }


    private String replaceWithFieldNames( RelNode rel, Object right ) {
        String str = right.toString();
        if ( str.contains( "$" ) ) {
            int offset = 0;
            for ( RelNode input : rel.getInputs() ) {
                for ( RelDataTypeField field : input.getRowType().getFieldList() ) {
                    String searchStr = "$" + (offset + field.getIndex());
                    int position = str.indexOf( searchStr );
                    if ( position >= 0 && (str.length() >= position + searchStr.length()) ) {
                        str = str.replace( searchStr, field.getName() );
                    }
                }
                offset = input.getRowType().getFieldList().size();
            }
        }
        return str;
    }


    private void put( Map<String, Object> map, String name, Object value ) {
        map.put( name, relJson.toJson( value ) );
    }


    private List<Object> explainInputs( List<RelNode> inputs ) {
        final List<Object> list = jsonBuilder.list();
        for ( RelNode input : inputs ) {
            String id = relIdMap.get( input );
            if ( id == null ) {
                input.explain( this );
                id = previousId;
            }
            list.add( id );
        }
        return list;
    }


    @Override
    public final void explain( RelNode rel, List<Pair<String, Object>> valueList ) {
        explain_( rel, valueList );
    }


    @Override
    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }


    @Override
    public RelWriter input( String term, RelNode input ) {
        return this;
    }


    @Override
    public RelWriter item( String term, Object value ) {
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
    public RelWriter itemIf( String term, Object value, boolean condition ) {
        if ( condition ) {
            item( term, value );
        }
        return this;
    }


    @Override
    public RelWriter done( RelNode node ) {
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
        map.put( "Plan", relList.get( relList.size() - 1 ) );
        return jsonBuilder.toJsonString( map );
    }
}

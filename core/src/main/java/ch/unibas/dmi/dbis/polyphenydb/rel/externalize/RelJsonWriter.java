/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.externalize;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.util.JsonBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


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


    public final void explain( RelNode rel, List<Pair<String, Object>> valueList ) {
        explain_( rel, valueList );
    }


    public SqlExplainLevel getDetailLevel() {
        return SqlExplainLevel.ALL_ATTRIBUTES;
    }


    public RelWriter input( String term, RelNode input ) {
        return this;
    }


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


    public RelWriter itemIf( String term, Object value, boolean condition ) {
        if ( condition ) {
            item( term, value );
        }
        return this;
    }


    public RelWriter done( RelNode node ) {
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf( values );
        values.clear();
        explain_( node, valuesCopy );
        return this;
    }


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

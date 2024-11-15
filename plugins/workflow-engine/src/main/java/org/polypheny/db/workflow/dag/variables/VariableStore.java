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

package org.polypheny.db.workflow.dag.variables;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class VariableStore implements ReadableVariableStore, WritableVariableStore {

    private final Map<String, JsonNode> variables = new HashMap<>();


    public ReadableVariableStore asReadable() {
        return this;
    }


    public WritableVariableStore asWritable() {
        return this;
    }


    @Override
    public void setVariable( String key, JsonNode value ) {
        variables.put( key, value );
    }


    @Override
    public void merge( ReadableVariableStore newStore ) {
        variables.putAll( newStore.getVariables() );
    }


    @Override
    public void clear() {
        variables.clear();
    }


    @Override
    public boolean contains( String key ) {
        return variables.containsKey( key );
    }


    @Override
    public JsonNode getVariable( String key ) {
        return variables.get( key );
    }


    @Override
    public Map<String, JsonNode> getVariables() {
        return Map.copyOf( variables );
    }


    // TODO: make sure to not change the existing JsonNode
    public JsonNode resolveVariables( JsonNode node ) {
        if ( node.isObject() ) {
            ObjectNode objectNode = (ObjectNode) node;
            if ( objectNode.size() == 1 && objectNode.has( VARIABLE_REF_FIELD ) ) {
                String variableRef = objectNode.get( VARIABLE_REF_FIELD ).asText();
                JsonNode replacement = variables.get( variableRef );

                // Replace the entire object with the value from the map, if it exists
                if ( replacement == null ) {
                    throw new IllegalArgumentException( "Cannot resolve variable with name: " + variableRef );
                }
                return replacement;
            } else {
                // Recursively process child fields
                Iterator<Entry<String, JsonNode>> fields = objectNode.fields();
                while ( fields.hasNext() ) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    objectNode.set( field.getKey(), resolveVariables( field.getValue() ) );
                }
                return objectNode;
            }
        } else if ( node.isArray() ) {
            // Recursively process child fields
            for ( int i = 0; i < node.size(); i++ ) {
                ((ArrayNode) node).set( i, resolveVariables( node.get( i ) ) );
            }
            return node;
        } else {
            return node;
        }
    }


    @Override
    public Map<String, JsonNode> resolveVariables( Map<String, JsonNode> nodes ) {
        Map<String, JsonNode> resolved = new HashMap<>();
        for ( Entry<String, JsonNode> entry : nodes.entrySet() ) {
            resolved.put( entry.getKey(), resolveVariables( entry.getValue() ) );
        }
        return Collections.unmodifiableMap( resolved );
    }


}

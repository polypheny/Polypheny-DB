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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.workflow.dag.edges.ControlEdge;
import org.polypheny.db.workflow.dag.edges.DataEdge;
import org.polypheny.db.workflow.dag.edges.Edge;
import org.polypheny.db.workflow.dag.edges.Edge.EdgeState;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

public class VariableStore implements ReadableVariableStore, WritableVariableStore {

    public static final String ERROR_MSG_KEY = "$errorMsg";
    public static final Set<String> RESERVED_KEYS = new HashSet<>( Set.of(
            ERROR_MSG_KEY
    ) );

    private static final JsonMapper mapper = new JsonMapper();

    private final Map<String, JsonNode> variables = new HashMap<>();


    public ReadableVariableStore asReadable() {
        return this;
    }


    public WritableVariableStore asWritable() {
        return this;
    }


    @Override
    public void setVariable( String key, JsonNode value ) {
        failIfReservedKey( key );
        if ( containsVariableRef( value ) ) {
            throw new IllegalArgumentException( "Setting a variable containing a variable reference is not allowed" );
        }
        variables.put( key, value );
    }


    @Override
    public void setVariable( String key, SettingValue value ) {
        failIfReservedKey( key );
        variables.put( key, value.toJson( mapper ) );
    }


    @Override
    public void setError( ObjectNode value ) {
        variables.put( ERROR_MSG_KEY, value );
    }


    @Override
    public void merge( ReadableVariableStore newStore ) {
        // does NOT fail with reserved keywords
        variables.putAll( newStore.getVariables() );
    }


    @Override
    public void clear() {
        variables.clear();
    }


    @Override
    public void reset( ReadableVariableStore newStore ) {
        clear();
        merge( newStore );
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
    public ObjectNode getError() {
        return (ObjectNode) getVariable( ERROR_MSG_KEY );
    }


    @Override
    public Map<String, JsonNode> getVariables() {
        return Map.copyOf( variables );
    }


    private void failIfReservedKey( String key ) {
        if ( RESERVED_KEYS.contains( key ) ) {
            throw new IllegalArgumentException( "Cannot use reserved key: " + key );
        }
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


    @Override
    public Map<String, Optional<JsonNode>> resolveAvailableVariables( Map<String, JsonNode> nodes ) {
        Map<String, Optional<JsonNode>> resolved = new HashMap<>();
        for ( Map.Entry<String, JsonNode> entry : nodes.entrySet() ) {
            try {
                resolved.put( entry.getKey(), Optional.of( resolveVariables( entry.getValue() ) ) );
            } catch ( IllegalArgumentException e ) {
                resolved.put( entry.getKey(), Optional.empty() );
            }
        }
        return Collections.unmodifiableMap( resolved );
    }


    /**
     * Clears this store, then merges all incoming variableStores if their corresponding edge is not inactive and not ignored.
     * Be careful, the resulting variables might not be consistent at a later point, as input stores might change.
     * <p>
     * The merge order is the following, with later stores able to overwrite variables written before:
     * 1. Stores from data inputs, ordered by their inPort
     * 2. Stores from success control edges (in arbitrary order)
     * 3. Stores from fail control edges (in arbitrary order)
     *
     * @param inEdges All input edges (data and control) to the activity
     */
    public void mergeInputStores( List<Edge> inEdges, int inPortCount ) {
        ReadableVariableStore[] dataToMerge = new ReadableVariableStore[inPortCount];
        Set<ReadableVariableStore> successToMerge = new HashSet<>();
        Set<ReadableVariableStore> failToMerge = new HashSet<>();

        for ( Edge edge : inEdges ) {
            if ( edge.getState() == EdgeState.INACTIVE ) {
                continue;
            }
            if ( edge instanceof DataEdge data ) {
                dataToMerge[data.getToPort()] = data.getFrom().getVariables();
            } else if ( edge instanceof ControlEdge control ) {
                if ( control.isIgnored() ) {
                    continue;
                }
                ReadableVariableStore variables = control.getFrom().getVariables();
                if ( variables == null ) {
                    continue;
                }

                if ( control.isOnSuccess() ) {
                    successToMerge.add( variables );
                } else {
                    failToMerge.add( variables );
                }
            } else {
                throw new IllegalArgumentException( "Unexpected Edge type" );
            }
        }

        this.clear();
        for ( ReadableVariableStore readableVariableStore : dataToMerge ) {
            if ( readableVariableStore != null ) {
                this.merge( readableVariableStore );
            }
        }
        successToMerge.forEach( this::merge );
        failToMerge.forEach( this::merge );
    }


    private static boolean containsVariableRef( JsonNode node ) {
        if ( node.isObject() ) {
            ObjectNode objectNode = (ObjectNode) node;
            if ( objectNode.size() == 1 && objectNode.has( VARIABLE_REF_FIELD ) ) {
                return true;
            } else {
                for ( JsonNode value : objectNode ) {
                    if ( containsVariableRef( value ) ) {
                        return true;
                    }
                }
            }
        } else if ( node.isArray() ) {
            for ( int i = 0; i < node.size(); i++ ) {
                if ( containsVariableRef( node.get( i ) ) ) {
                    return true;
                }
            }
        }
        return false;

    }


}

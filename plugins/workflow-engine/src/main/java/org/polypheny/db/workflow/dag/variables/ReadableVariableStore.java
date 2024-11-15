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
import java.util.Map;

public interface ReadableVariableStore {

    String VARIABLE_REF_FIELD = "_variableRef";

    boolean contains( String key );

    JsonNode getVariable( String key );

    /**
     * Get an unmodifiable snapshot of the underlying variables map.
     *
     * @return an unmodifiable map containing mappings for all stored variables.
     */
    Map<String, JsonNode> getVariables();

    /**
     * Recursively replaces any variable references in the specified JsonNode with the value stored in this store.
     * Variable references are indicated by objects that contain a single field with name equal to the value of {@code VARIABLE_REF_FIELD}.
     *
     * @param node The node to be resolved recursively
     * @return a JsonNode with any variable references replaced by their value stored in this store.
     * @throws IllegalArgumentException if a variable reference cannot be resolved in the variableMap
     */
    JsonNode resolveVariables( JsonNode node );


    Map<String, JsonNode> resolveVariables( Map<String, JsonNode> nodes );

}

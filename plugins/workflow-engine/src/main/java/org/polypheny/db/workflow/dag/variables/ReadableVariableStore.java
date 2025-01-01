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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Optional;

public interface ReadableVariableStore {

    /**
     * A variable reference is given as an object with a VARIABLE_REF_FIELD key and the reference as a string value.
     * A reference corresponds to a JsonPointer (https://datatracker.ietf.org/doc/html/rfc6901).
     * Note that "/" and "~" in variable names need to be escaped as ~1 and ~0 respectively.
     * The leading "/" is optional, as the first referenced object must always correspond to a variable name.
     * Examples of valid references:
     * <ul>
     *     <li>{@code "myVariable"}</li>
     *     <li>{@code "/myVariable"}</li>
     *     <li>{@code "myVariable/names"}</li>
     *     <li>{@code "myVariable/names/0"}</li>
     *     <li>{@code "my~1escaped~0variable~1example"}</li>
     * </ul>
     */
    String VARIABLE_REF_FIELD = "$ref";

    boolean contains( String key );

    JsonNode getVariable( String key );

    ObjectNode getError();

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


    /**
     * Resolves variables in the given map by recursively replacing any variable references with their resolved values.
     *
     * @param nodes The nodes to be resolved
     * @return an immutable map with JsonNodes that have any variable references replaced by their value stored in this store.
     * @throws IllegalArgumentException if any {@link JsonNode} contains an unresolved variable reference.
     */
    Map<String, JsonNode> resolveVariables( Map<String, JsonNode> nodes );

    /**
     * Resolves variables in the given map by recursively replacing any variable references with their resolved values.
     * If a variable cannot be resolved, inserts {@link Optional#empty()} for that variable.
     *
     * @param nodes The nodes to be resolved
     * @return an immutable map with resolved variables wrapped in {@link Optional}.
     */
    Map<String, Optional<JsonNode>> resolveAvailableVariables( Map<String, JsonNode> nodes );

}

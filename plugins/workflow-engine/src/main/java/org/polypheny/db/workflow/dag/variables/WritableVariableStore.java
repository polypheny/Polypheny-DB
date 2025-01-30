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
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

public interface WritableVariableStore {

    /**
     * Sets a variable in the store with the given key and (resolved) value.
     *
     * @param key the key for the variable.
     * @param value the {@link JsonNode} value to associate with the key.
     * @throws IllegalArgumentException if the value contains a variable reference and is therefore not resolved.
     */
    void setVariable( String key, JsonNode value );

    /**
     * Sets a variable in the store with the given key and (resolved) value.
     *
     * @param key the key for the variable.
     * @param obj the object to be mapped to a {@link JsonNode} and associated with the key
     * @throws IllegalArgumentException if the value contains a variable reference and is therefore not resolved.
     */
    void setVariable( String key, Object obj );

    /**
     * Sets a variable in the store with the given key and corresponding json representation
     * of the supplied value.
     *
     * @param key the key for the variable.
     * @param value the value to associate with the key.
     */
    void setVariable( String key, SettingValue value );

    void setError( ObjectNode value );

    void setEnvVariable( String key, JsonNode envVariable );

    /**
     * Merge this variableStore with the specified store.
     * In the case of duplicates, newStore takes priority.
     *
     * @param newStore the store that is merged with this store
     */
    void merge( ReadableVariableStore newStore );

    void clear();

    /**
     * Clears the store and then sets the specified workflow variables
     *
     * @param workflowVariables the workflow variables
     */
    void reset( Map<String, JsonNode> workflowVariables );

}

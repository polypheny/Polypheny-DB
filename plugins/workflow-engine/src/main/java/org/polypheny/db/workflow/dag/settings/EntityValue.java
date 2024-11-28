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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.Objects;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class EntityValue implements SettingValue {

    String namespace;
    String name; // for graphs, only name is important


    /**
     * Converts the given {@link JsonNode} to an {@link EntityValue}.
     *
     * @param node The {@link JsonNode} to convert. It must represent an entity value.
     * @return A new {@link EntityValue} instance containing the integer value.
     * @throws IllegalArgumentException if the {@link JsonNode} cannot be converted.
     */
    public static EntityValue of( JsonNode node ) {
        JsonNode nsNode = node.get( "namespace" );
        JsonNode nameNode = node.get( "name" );
        try {
            return new EntityValue( Objects.requireNonNull( nsNode.textValue() ), Objects.requireNonNull( nameNode.textValue() ) );
        } catch ( NullPointerException e ) {
            throw new IllegalArgumentException( node + " does not represent an entity" );
        }
    }


    @Override
    public JsonNode toJson( JsonMapper mapper ) {
        return mapper.createObjectNode()
                .put( "namespace", namespace )
                .put( "name", name );
    }

}

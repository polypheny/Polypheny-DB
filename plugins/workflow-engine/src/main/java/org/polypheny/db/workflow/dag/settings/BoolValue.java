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
import com.fasterxml.jackson.databind.node.BooleanNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class BoolValue implements SettingValue {

    @Getter(AccessLevel.NONE)
    boolean value;


    public boolean getValue() {
        return value;
    }


    /**
     * Converts the given {@link JsonNode} to an {@link BoolValue}.
     *
     * @param node The {@link JsonNode} to convert. It must represent a boolean value.
     * @return A new {@link BoolValue} instance containing the boolean value.
     * @throws IllegalArgumentException if the {@link JsonNode} cannot be converted to a boolean.
     */
    public static BoolValue of( JsonNode node ) {
        if ( !node.isBoolean() ) {
            throw new IllegalArgumentException( node + " does not represent a boolean" );
        }
        return new BoolValue( node.asBoolean() );
    }


    @Override
    public JsonNode toJson() {
        return BooleanNode.valueOf( value );
    }

}

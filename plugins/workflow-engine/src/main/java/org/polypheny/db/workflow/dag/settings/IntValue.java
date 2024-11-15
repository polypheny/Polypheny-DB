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
import com.fasterxml.jackson.databind.node.IntNode;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class IntValue implements SettingValue {

    int value;


    /**
     * Converts the given {@link JsonNode} to an {@link IntValue}.
     *
     * @param node The {@link JsonNode} to convert. It must represent an integer value.
     * @return A new {@link IntValue} instance containing the integer value.
     * @throws IllegalArgumentException if the {@link JsonNode} cannot be converted to an integer.
     */
    public static IntValue of( JsonNode node ) {
        if ( !node.canConvertToInt() ) {
            throw new IllegalArgumentException( node + " does not represent an integer" );
        }
        return new IntValue( node.intValue() );
    }


    @Override
    public JsonNode toJson( JsonMapper mapper ) {
        return IntNode.valueOf( value );
    }

}

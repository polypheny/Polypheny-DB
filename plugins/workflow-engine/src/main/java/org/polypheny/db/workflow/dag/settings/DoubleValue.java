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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class DoubleValue implements SettingValue {

    double value;


    /**
     * Converts the given {@link JsonNode} to an {@link DoubleValue}.
     *
     * @param node The {@link JsonNode} to convert. It must represent a number.
     * @return A new {@link DoubleValue} instance containing the double value.
     * @throws IllegalArgumentException if the {@link JsonNode} cannot be converted to a double.
     */
    public static DoubleValue of( JsonNode node ) {
        if ( !node.isNumber() ) {
            throw new IllegalArgumentException( node + " does not represent a number" );
        }
        return new DoubleValue( node.doubleValue() );
    }


    @Override
    public JsonNode toJson( ObjectMapper mapper ) {
        return DoubleNode.valueOf( value );
    }

}

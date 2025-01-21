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
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.NonNull;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class ListValue<T extends SettingValue> implements SettingValue {

    @NonNull
    List<T> values;


    /**
     * Converts a JsonNode (either a JSON array or a single value) into a ListValue of SettingValue objects.
     *
     * @param <T> The type of SettingValue to map each JSON node to.
     * @param node The JsonNode to be converted, either a single value or a JSON array.
     * @param mapper A function that maps a JsonNode to a SettingValue of type T.
     * @return A ListValue containing a list of mapped SettingValue objects.
     * @throws IllegalArgumentException if the node cannot be converted
     */
    public static <T extends SettingValue> ListValue<T> of( JsonNode node, Function<JsonNode, T> mapper ) {
        List<T> list = new ArrayList<>();
        if ( node.isArray() ) {
            for ( JsonNode entry : node ) {
                list.add( mapper.apply( entry ) );
            }
        } else {
            list.add( mapper.apply( node ) );
        }
        return new ListValue<>( list );
    }


    public static <T extends SettingValue, E> ListValue<T> of( List<E> list, Function<E, T> mapper ) {
        List<T> values = new ArrayList<>();
        for ( E entry : list ) {
            values.add( mapper.apply( entry ) );
        }
        return new ListValue<>( values );
    }


    public static <T extends SettingValue> ListValue<T> of() {
        return new ListValue<>( List.of() );
    }


    public static <T extends SettingValue> ListValue<T> of( T entry ) {
        return new ListValue<>( List.of( entry ) );
    }


    @Override
    public JsonNode toJson() {
        ArrayNode node = MAPPER.createArrayNode();
        for ( SettingValue value : values ) {
            node.add( value.toJson() );
        }
        return node;
    }

}

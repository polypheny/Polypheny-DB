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
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Objects;
import lombok.NonNull;
import lombok.Value;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class StringValue implements SettingValue {

    @NonNull
    String value;


    public static StringValue of( String s ) {

        return new StringValue( Objects.requireNonNullElse( s, "" ) );
    }


    public static StringValue of( JsonNode node ) {
        if ( !node.isTextual() ) {
            throw new IllegalArgumentException( node + " does not represent a string." );
        }
        return new StringValue( node.textValue() );
    }


    @Override
    public JsonNode toJson( JsonMapper mapper ) {
        return TextNode.valueOf( value );
    }

}
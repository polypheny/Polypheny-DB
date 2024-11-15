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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.annotations.StringSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class StringSettingDef extends SettingDef {

    boolean isList;


    public StringSettingDef( StringSetting a ) {
        super( SettingType.STRING, a.key(), a.displayName(), a.description(), getDefaultValue( a.defaultValue(), a.isList() ), a.group(), a.subGroup(), a.position(), a.subOf() );
        this.isList = a.isList();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        if ( isList ) {
            return ListValue.of( node, StringValue::of );
        }
        return StringValue.of( node );
    }


    private static SettingValue getDefaultValue( String s, boolean isList ) {
        if ( isList ) {
            if ( s.isEmpty() ) {
                return ListValue.of();
            } else {
                return ListValue.of( StringValue.of( s ) );
            }
        }
        return StringValue.of( s );
    }

}

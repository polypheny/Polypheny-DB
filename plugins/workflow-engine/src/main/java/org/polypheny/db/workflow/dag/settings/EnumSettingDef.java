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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class EnumSettingDef extends SettingDef {

    String[] options;
    String[] displayOptions;
    String[] displayDescriptions;
    String label;

    @JsonIgnore
    Set<String> optionsSet;


    public EnumSettingDef( EnumSetting a ) {
        super( SettingType.ENUM, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultValue() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        assert a.options().length >= a.displayOptions().length : "Too many display options";
        options = a.options();
        optionsSet = Set.of( a.options() );
        displayOptions = Arrays.copyOf( options, options.length );
        for ( int i = 0; i < a.displayOptions().length; i++ ) {
            String d = a.displayOptions()[i];
            if ( !d.isEmpty() ) {
                displayOptions[i] = d;
            }
        }
        displayDescriptions = a.displayDescriptions().length == 0 ? null : a.displayDescriptions();
        label = a.label();

        assert displayDescriptions == null || displayDescriptions.length == options.length : "Invalid number of display descriptions";
        assert optionsSet.contains( a.defaultValue() ) : "The default value '" + a.defaultValue() + "' is not a valid option";
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return StringValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof StringValue s ) {
            if ( !optionsSet.contains( s.getValue() ) ) {
                throwInvalid( "Value '" + s.getValue() + "' is not a valid option." );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a BoolValue" );
    }


    private static SettingValue getDefaultValue( String value ) {
        return new StringValue( value );
    }

}

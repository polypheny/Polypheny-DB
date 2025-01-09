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
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.IntSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class IntSettingDef extends SettingDef {

    boolean isList;
    int minValue;
    int maxValue;


    public IntSettingDef( IntSetting a ) {
        super( SettingType.INT, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultValue(), a.isList() ), a.group(), a.subGroup(), a.position(), a.subOf() );
        this.isList = a.isList();
        this.minValue = a.min();
        this.maxValue = a.max();

        assert minValue < maxValue;
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        if ( isList ) {
            return ListValue.of( node, IntValue::of );
        }
        return IntValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof IntValue intValue ) {
            validateIntValue( intValue );
            return;
        } else if ( value instanceof ListValue<? extends SettingValue> list ) {
            if ( !isList ) {
                throw new IllegalArgumentException( "Value must not be a list" );
            }
            for ( SettingValue v : list.getValues() ) {
                validateIntValue( (IntValue) v );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a IntValue" );
    }


    private void validateIntValue( IntValue value ) throws InvalidSettingException {
        int i = value.getValue();
        if ( i < minValue ) {
            throw new InvalidSettingException( "Int must not be smaller than " + minValue, getKey() );
        } else if ( i > maxValue ) {
            throw new InvalidSettingException( "Int must not be larger than " + maxValue, getKey() );
        }
    }


    private static SettingValue getDefaultValue( int value, boolean isList ) {
        if ( isList ) {
            if ( value == 0 ) {
                return ListValue.of();
            } else {
                return ListValue.of( new IntValue( value ) );
            }
        }
        return new IntValue( value );
    }

}

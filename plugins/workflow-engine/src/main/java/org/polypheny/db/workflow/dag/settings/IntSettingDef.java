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

    int minValue;
    int maxValue;


    public IntSettingDef( IntSetting a ) {
        super( SettingType.INT, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultValue() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.minValue = a.min();
        this.maxValue = a.max();

        assert minValue < maxValue;
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return IntValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof IntValue intValue ) {
            validateIntValue( intValue );
            return;
        }
        throw new IllegalArgumentException( "Value is not a IntValue" );
    }


    private void validateIntValue( IntValue value ) throws InvalidSettingException {
        int i = value.getValue();
        if ( i < minValue ) {
            throwInvalid( "Value must not be smaller than " + minValue );
        } else if ( i > maxValue ) {
            throwInvalid( "Value must not be larger than " + maxValue );
        }
    }


    private static SettingValue getDefaultValue( int value ) {
        return new IntValue( value );
    }

}

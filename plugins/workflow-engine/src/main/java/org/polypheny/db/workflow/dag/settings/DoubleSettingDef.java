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
import org.polypheny.db.workflow.dag.annotations.DoubleSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class DoubleSettingDef extends SettingDef {

    double minValue;
    double maxValue;


    public DoubleSettingDef( DoubleSetting a ) {
        super( SettingType.DOUBLE, a.key(), a.displayName(), a.description(), getDefaultValue( a.defaultValue() ), a.group(), a.subGroup(), a.position(), a.subOf() );
        minValue = a.min();
        maxValue = a.max();

        assert minValue < maxValue;

    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return DoubleValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof DoubleValue doubleValue ) {
            double d = doubleValue.getValue();
            if ( d < minValue ) {
                throw new InvalidSettingException( "Value must not be smaller than " + minValue, getKey() );
            } else if ( d > maxValue ) {
                throw new InvalidSettingException( "Value must not be larger than " + maxValue, getKey() );
            }
        } else {
            throw new IllegalArgumentException( "Value is not a DoubleValue" );
        }
    }


    private static SettingValue getDefaultValue( double value ) {
        return new DoubleValue( value );
    }

}

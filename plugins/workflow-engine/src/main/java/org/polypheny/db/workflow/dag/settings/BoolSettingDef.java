/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.workflow.dag.annotations.BoolSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class BoolSettingDef extends SettingDef {


    public BoolSettingDef( BoolSetting a ) {
        super( SettingType.BOOLEAN, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultValue() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return BoolValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( !(value instanceof BoolValue) ) {
            throw new IllegalArgumentException( "Value is not a BoolValue" );
        }
    }


    private static SettingValue getDefaultValue( boolean value ) {
        return new BoolValue( value );
    }

}

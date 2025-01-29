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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.CollationSetting;
import org.polypheny.db.workflow.dag.settings.CollationValue.FieldCollation;

@EqualsAndHashCode(callSuper = true)
@Value
public class CollationSettingDef extends SettingDef {

    int targetInput;
    boolean allowRegex;


    public CollationSettingDef( CollationSetting a ) {
        super( SettingType.COLLATION, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.allowRegex() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        targetInput = a.targetInput();
        allowRegex = a.allowRegex();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, CollationValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        Set<String> names = new HashSet<>();
        if ( value instanceof CollationValue collation ) {
            for ( FieldCollation field : collation.getFields() ) {
                if ( field.isRegex() ) {
                    if ( !allowRegex ) {
                        throwInvalid( "Regex is not permitted" );
                    }
                    try {
                        field.getCompiledPattern();
                    } catch ( PatternSyntaxException e ) {
                        throwInvalid( "Regex syntax is invalid: " + field.getName() + " (" + e.getMessage() + ")" );
                    }
                }
                if ( field.getName().isBlank() ) {
                    throwInvalid( "Collation name must not be blank" );
                }
                if ( names.contains( field.getName() ) ) {
                    throwInvalid( "Duplicate name: " + field.getName() );
                }
                names.add( field.getName() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a CollationValue" );
    }


    private static SettingValue getDefaultValue( boolean allowRegex ) {
        return new CollationValue( List.of() );
    }

}

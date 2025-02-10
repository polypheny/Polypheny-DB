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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.StringSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class StringSettingDef extends SettingDef {

    int minLength;
    int maxLength;
    AutoCompleteType autoComplete;
    int autoCompleteInput;
    boolean nonBlank;
    boolean containsRegex;
    boolean textEditor;
    String textEditorLanguage;


    public StringSettingDef( StringSetting a ) {
        super( SettingType.STRING, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultValue() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.minLength = a.minLength();
        this.maxLength = a.maxLength();
        this.autoComplete = a.autoCompleteType();
        this.autoCompleteInput = a.autoCompleteInput();
        this.nonBlank = a.nonBlank();
        this.containsRegex = a.containsRegex();
        this.textEditor = a.textEditor();
        this.textEditorLanguage = a.language();

        assert minLength < maxLength;
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return StringValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof StringValue stringValue ) {
            validateStringValue( stringValue );
            return;
        }
        throw new IllegalArgumentException( "Value is not a SettingValue" );
    }


    private void validateStringValue( StringValue value ) throws InvalidSettingException {
        String s = value.getValue();
        if ( s.length() < minLength ) {
            throwInvalid( "String must have a length of at least " + minLength );
        } else if ( s.length() >= maxLength ) {
            throwInvalid( "String must have a length of less than " + maxLength );
        } else if ( nonBlank && s.isBlank() ) {
            throwInvalid( "String must not be empty" );
        } else if ( containsRegex ) {
            try {
                Pattern.compile( s );
            } catch ( PatternSyntaxException e ) {
                throwInvalid( "Invalid Regex: " + e.getMessage() );
            }
        }
    }


    private static SettingValue getDefaultValue( String s ) {
        return StringValue.of( s );
    }


    public enum AutoCompleteType {
        NONE,
        FIELD_NAMES,
        VALUES,
        ADAPTERS;
    }

}

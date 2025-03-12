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
import java.util.List;
import java.util.regex.PatternSyntaxException;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.FieldRenameSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;

@EqualsAndHashCode(callSuper = true)
@Value
public class FieldRenameSettingDef extends SettingDef {

    SelectMode defaultMode;
    boolean allowRegex;
    boolean allowIndex;
    int targetInput;
    boolean forLabels; // when renaming a graph, autocomplete needs to know if labels or props are renamed


    public FieldRenameSettingDef( FieldRenameSetting a ) {
        super( SettingType.FIELD_RENAME, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultMode() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.defaultMode = a.defaultMode();
        this.allowRegex = a.allowRegex();
        this.allowIndex = a.allowIndex();
        this.targetInput = a.targetInput();
        this.forLabels = a.forLabels();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, FieldRenameValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof FieldRenameValue rename ) {
            if ( rename.getMode() == SelectMode.REGEX ) {
                if ( !allowRegex ) {
                    throwInvalid( "Regex mode is not allowed" );
                }
                try {
                    rename.validateRegex();
                } catch ( PatternSyntaxException e ) {
                    throwInvalid( "Invalid regex: " + e.getMessage() );
                }
            } else if ( rename.getMode() == SelectMode.INDEX ) {
                if ( !allowIndex ) {
                    throwInvalid( "Index mode is not allowed" );
                }
                try {
                    rename.validateIndex(); // the max index can only be caught later, in previewOutTypes
                } catch ( NumberFormatException e ) {
                    throwInvalid( "Invalid index: " + e.getMessage() );
                }
            }

            if ( rename.hasDuplicates() ) {
                throwInvalid( "Duplicate source fields are not permitted" );
            } else if ( rename.getMode() != SelectMode.REGEX && rename.hasEmptyRule() ) {
                // REGEX can replace with empty string, as only substrings of field names might get replaced
                throwInvalid( "Empty rules are not permitted" );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a FieldRenameValue" );

    }


    private static SettingValue getDefaultValue( SelectMode defaultMode ) {
        return new FieldRenameValue( List.of(), defaultMode, false );
    }

}

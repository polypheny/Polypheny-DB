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
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class FieldSelectSettingDef extends SettingDef {

    boolean simplified;
    boolean reorder;
    boolean defaultAll;
    int targetInput;
    boolean forLabels; // when selecting in a graph, autocomplete needs to know if labels or props are selected


    public FieldSelectSettingDef( FieldSelectSetting a ) {
        super( SettingType.FIELD_SELECT, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultUnspecified() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.simplified = a.simplified();
        this.reorder = a.reorder();
        this.defaultAll = a.defaultAll();
        this.targetInput = a.targetInput();
        this.forLabels = a.forLabels();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        if ( node == null || node.isNull() ) {
            return new FieldSelectValue( List.of(), List.of(), -1 );
        } else if ( node.isArray() ) { // support deserialization when only values to include are provided
            List<String> include = new ArrayList<>();
            for ( JsonNode item : node ) {
                include.add( item.asText() );
            }
            return new FieldSelectValue( include, List.of(), -1 );
        }
        return SettingValue.fromJson( node, FieldSelectValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof FieldSelectValue select ) {
            if ( simplified ) {
                if ( !select.getExclude().isEmpty() ) {
                    throwInvalid( "Exclude fields are not permitted in simplified fieldSelect" );
                }
                if ( select.includeUnspecified() ) {
                    throwInvalid( "Unspecified fields cannot be included in simplified fieldSelect" );
                }
            } else {
                if ( select.getInclude().isEmpty() && select.getExclude().isEmpty() && !select.includeUnspecified() ) {
                    throwInvalid( "At least one field must be included or excluded" );
                }
            }
            for ( String include : select.getInclude() ) {
                if ( select.getExclude().contains( include ) ) {
                    throwInvalid( "Field \"" + include + "\" is both included and excluded" );
                }
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a FieldSelectValue" );

    }


    private static SettingValue getDefaultValue( boolean includeUnspecified ) {
        return new FieldSelectValue( List.of(), List.of(), includeUnspecified ? 0 : -1 );
    }

}

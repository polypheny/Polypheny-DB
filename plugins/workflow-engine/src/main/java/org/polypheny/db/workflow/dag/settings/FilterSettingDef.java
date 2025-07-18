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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.settings.FilterValue.Operator;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;

@EqualsAndHashCode(callSuper = true)
@Value
public class FilterSettingDef extends SettingDef {

    SelectMode[] modes;
    List<FilterValue.Operator> operators;
    int targetInput;


    public FilterSettingDef( FilterSetting a ) {
        super( SettingType.FILTER, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.modes()[0] ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.modes = a.modes();
        List<FilterValue.Operator> operators = new ArrayList<>( Arrays.asList( Operator.values() ) );
        for ( FilterValue.Operator op : a.excludedOperators() ) {
            operators.remove( op );
        }
        this.operators = Collections.unmodifiableList( operators );
        this.targetInput = a.targetInput();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, FilterValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof FilterValue filter ) {
            try {
                filter.validate( operators );
            } catch ( IllegalArgumentException e ) {
                throwInvalid( e.getMessage() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a FilterValue" );

    }


    private static SettingValue getDefaultValue( SelectMode mode ) {
        return new FilterValue( List.of(), mode, false );
    }

}

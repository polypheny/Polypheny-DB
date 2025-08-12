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
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.AggregateSetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class AggregateSettingDef extends SettingDef {

    int targetInput;
    List<String> allowedFunctions;


    public AggregateSettingDef( AggregateSetting a ) {
        super( SettingType.AGGREGATE, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.allowedFunctions() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        targetInput = a.targetInput();
        allowedFunctions = new ArrayList<>();
        for ( String f : a.allowedFunctions() ) {
            if ( OperatorName.valueOf( f ).getClazz() != AggFunction.class ) {
                throw new IllegalArgumentException( "Not an AggFunction: " + f );
            }
            allowedFunctions.add( f );
        }
        assert !allowedFunctions.isEmpty() : "At least one function is required";
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, AggregateValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof AggregateValue agg ) {
            try {
                agg.validate( allowedFunctions );
            } catch ( IllegalArgumentException e ) {
                throwInvalid( e.getMessage() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not an AggregateValue" );
    }


    private static SettingValue getDefaultValue( String[] allowedFunctions ) {
        return new AggregateValue( List.of() );
    }

}

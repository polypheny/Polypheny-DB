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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.settings.CastValue.SingleCast;

@EqualsAndHashCode(callSuper = true)
@Value
public class CastSettingDef extends SettingDef {

    int targetInput;
    PolyType defaultType;
    boolean allowDuplicateSource;
    boolean allowTarget;
    boolean allowJson;
    boolean singleCast;


    public CastSettingDef( CastSetting a ) {
        super( SettingType.CAST, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), constructDefault( a.singleCast(), a.defaultType() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.targetInput = a.targetInput();
        this.defaultType = a.defaultType();
        this.allowDuplicateSource = a.duplicateSource();
        this.allowTarget = a.allowTarget();
        this.allowJson = a.allowJson();
        this.singleCast = a.singleCast();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return SettingValue.fromJson( node, CastValue.class );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof CastValue cast ) {
            try {
                cast.validate( allowDuplicateSource, allowTarget, allowJson, singleCast );
            } catch ( IllegalArgumentException e ) {
                throwInvalid( e.getMessage() );
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not a CastValue" );

    }


    private static SettingValue constructDefault( boolean singleCast, PolyType defaultType ) {
        if ( singleCast ) {
            return new CastValue( List.of(
                    SingleCast.of( "", defaultType, true )
            ) );
        }
        return new CastValue( List.of() );
    }

}

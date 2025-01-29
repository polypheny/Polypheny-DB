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
import java.util.Objects;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.EntitySetting;

@EqualsAndHashCode(callSuper = true)
@Value
public class EntitySettingDef extends SettingDef {

    DataModel dataModel;
    boolean mustExist;
    private static final Pattern nameValidation = Pattern.compile( "^[a-zA-Z_][a-zA-Z0-9_]*$" ); // namespace must be lower case, but this is done automatically


    public EntitySettingDef( EntitySetting a ) {
        super( SettingType.ENTITY, a.key(), a.displayName(), a.shortDescription(), a.longDescription(), getDefaultValue( a.defaultNamespace(), a.defaultName() ),
                a.group(), a.subGroup(), a.pos(), a.subPointer(), a.subValues() );
        this.dataModel = a.dataModel();
        this.mustExist = a.mustExist();
    }


    @Override
    public SettingValue buildValue( JsonNode node ) {
        return EntityValue.of( node );
    }


    @Override
    public void validateValue( SettingValue value ) throws InvalidSettingException {
        if ( value instanceof EntityValue entityValue ) {
            if ( mustExist ) {
                LogicalEntity entity = switch ( dataModel ) {
                    case RELATIONAL -> entityValue.getTable();
                    case DOCUMENT -> entityValue.getCollection();
                    case GRAPH -> entityValue.getGraph();
                };
                if ( entity == null ) {
                    throwInvalid( "Entity \"" + entityValue.getNamespace() + "." + entityValue.getName() + "\" does not exist" );
                }
            }
            if ( entityValue.getNamespace().isBlank() ) {
                throwInvalid( "Namespace must not be empty" );
            }
            if ( !nameValidation.matcher( entityValue.getNamespace() ).matches() ) {
                throwInvalid( "The namespace may only contain alphanumeric characters and underscores" );
            }
            if ( dataModel != DataModel.GRAPH ) {
                if ( entityValue.getName().isBlank() ) {
                    throwInvalid( "Entity name must not be empty" );
                }
                if ( !nameValidation.matcher( entityValue.getName() ).matches() ) {
                    throwInvalid( "The entity name may only contain alphanumeric characters and underscores" );
                }
            }
            return;
        }
        throw new IllegalArgumentException( "Value is not an EntityValue" );

    }


    private static SettingValue getDefaultValue( String namespace, String name ) {
        return new EntityValue( Objects.requireNonNullElse( namespace, "" ), Objects.requireNonNullElse( name, "" ) );
    }

}

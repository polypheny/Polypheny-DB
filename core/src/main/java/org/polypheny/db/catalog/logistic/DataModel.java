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

package org.polypheny.db.catalog.logistic;

import lombok.Getter;
import org.polypheny.db.schema.trait.ModelTrait;

@Getter
public enum DataModel {
    RELATIONAL( 1 ),
    DOCUMENT( 2 ),
    GRAPH( 3 );

    // GRAPH, DOCUMENT, ...
    public final int id;


    DataModel( int id ) {
        this.id = id;
    }


    public static DataModel getDefault() {
        //return (NamespaceType) ConfigManager.getInstance().getConfig( "runtime/defaultSchemaModel" ).getEnum();
        return DataModel.RELATIONAL;
    }


    public static DataModel getById( final int id ) {
        for ( DataModel t : values() ) {
            if ( t.id == id ) {
                return t;
            }
        }
        throw new RuntimeException( "Unknown NamespaceType with id: " + id );
    }


    public static DataModel getByName( final String name ) {
        for ( DataModel t : values() ) {
            if ( t.name().equalsIgnoreCase( name ) ) {
                return t;
            }
        }
        throw new RuntimeException( "Unknown NamespaceType with name: " + name );
    }


    public ModelTrait getModelTrait() {
        if ( this == DataModel.RELATIONAL ) {
            return ModelTrait.RELATIONAL;
        } else if ( this == DataModel.DOCUMENT ) {
            return ModelTrait.DOCUMENT;
        } else if ( this == DataModel.GRAPH ) {
            return ModelTrait.GRAPH;
        }
        throw new RuntimeException( "Not found a suitable NamespaceType." );
    }
}
